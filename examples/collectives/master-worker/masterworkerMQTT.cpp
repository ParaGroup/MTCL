/*
 * Native MQTT master-worker microbenchmark (fan-out via shared subscription, fan-in via a single result topic).
 *
 * Requirements:
 * - A running MQTT broker reachable at BROKER_ADDRESS (default tcp://localhost:1883).
 * - The broker must support shared subscriptions for the fan-out pattern.
 *   This test uses the subscription filter: "$share/group1/taskTopic".
 * - Paho MQTT C++ and C libraries installed (mqtt/client.h, -lpaho-mqttpp3, -lpaho-mqtt3a or -lpaho-mqtt3as).
 *
 * Build (example):
 *   mpicxx -O3 -std=c++17 -Wall -DNDEBUG \
 *     masterworkerMQTT.cpp \
 *     -I<PAHO_INCLUDE_DIR> -L<PAHO_LIB_DIR> -Wl,-rpath,<PAHO_LIB_DIR> \
 *     -lpaho-mqttpp3 -lpaho-mqtt3a -pthread -lrt \
 *     -o masterworkerMQTT
 *  
 * or, by using the Makefile
 *   make TPROTOCOL=MQTT masterworkerMQTT
 *
 * Run:
 *   mpirun -np <N> ./masterworkerMQTT <ntasks> <sleep_us> <broker_address>
 *
 * Roles:
 * - MPI rank 0 is the Master.
 * - MPI ranks 1..N-1 are Workers.
 */
#include <iostream>
#include <thread>
#include <string>
#include <cstring>
#include <unistd.h>
#include <mqtt/client.h>
#include <chrono>
#include <cstdint>
#include "mpi.h"
using namespace std::chrono;

size_t SLEEP_TIME_US       = 500;
size_t NTASKS              = 1000;
std::string BROKER_ADDRESS = "tcp://localhost:1883";

using result_t = int;
using task_t = int;

task_t generateTask(){
    static int counter = 0;
    return ++counter;
}

result_t processTask(task_t t){
    return t;
}

void processResults(mqtt::client& c) {
    c.subscribe("resultTopic", 1);
    result_t result;
    for (size_t i = 0; i < NTASKS; i++){
        auto msg = c.consume_message();
        memcpy(&result, msg->get_payload().c_str(), sizeof(result_t));
        // process the result
    }
}

int main(int argc, char**argv) {
	if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <ntasks> <sleep_us> [<broker_address>=" << BROKER_ADDRESS << "]\n";
        return -1;
    }
	NTASKS         = (size_t)std::strtoull(argv[1], nullptr, 10);
    SLEEP_TIME_US  = (size_t)std::strtoull(argv[2], nullptr, 10);
	if (argc == 4)
		BROKER_ADDRESS = argv[3];
		
    MPI_Init(&argc, &argv);
    int rank=-1, size = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	if (size < 2) {
        std::cerr << "[TEST] Need at least 2 MPI ranks\n";
        MPI_Finalize();
        return 1;
    }
	
    std::string peer;
    if (rank == 0) 
        peer = "Master";
    else
        peer = "Worker"+std::to_string(rank);

    //std::string peer = argv[1];
    mqtt::client client(BROKER_ADDRESS, peer);
    client.connect();

    MPI_Barrier(MPI_COMM_WORLD);
    if (peer == "Master"){ // Master
        auto start = high_resolution_clock::now();
        std::thread t(processResults, std::ref(client));
        
        for (size_t i = 0; i < NTASKS; i++){
            task_t task = generateTask();
            client.publish("taskTopic", &task, sizeof(task_t), 1, false); // Send message
        }
        client.publish("taskTopicEND", "END", 3, 1, false);

        t.join();
        auto stop = high_resolution_clock::now();
        std::cout << "Time: " << duration_cast<milliseconds>(stop - start).count() << std::endl;
    } else { // Worker 
        client.subscribe("$share/group1/taskTopic", 1); // MQTT 5 shared subscribe (Fan-out)
        client.subscribe("taskTopicEND", 1);
        size_t processedTasks = 0;
        do {
            task_t task;
            auto msg = client.consume_message();
            if (msg->get_payload_str() == "END")
                break;
    
            memcpy(&task, msg->get_payload().c_str(), sizeof(task_t));
            
            usleep(SLEEP_TIME_US);
            
            processedTasks++;
            result_t result = processTask(task);
            client.publish("resultTopic", &result, sizeof(result_t), 1, false);
        } while(true);

        std::cout << peer << " processed " << processedTasks << " tasks\n";
    }

    client.disconnect();
    MPI_Finalize();
    return 0;
}
