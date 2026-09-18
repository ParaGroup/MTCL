/**
 * Client-Server model, each client sends 26 messages (one for each letter of the
 * alphabet). The server receive the payload, elaborate it and sends it back.
 * This version of the server is Synchronous with only one thread advancing the requests
 * and the I/O thread(if active) to handle connections.
 * How to run:
 *   Server: ./test_multiclient_sync 0
 *   Client: ./test_multiclient_sync 1 [server_host] [threads_num]
 */


#include <cerrno>
#include <csignal>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <chrono>

#include <sys/wait.h>
#include <unistd.h>
#include <execution>

#include "mtcl.hpp"

using namespace MTCL;

int MSG_SIZE = 5*1024*1024;

struct ClientSession{
    size_t id;
    ClientSession(size_t client_id) : id(client_id) {}
};

void server() {
    std::unordered_map<size_t, ClientSession> active_clients;

    if (Manager::listen("TCP:0.0.0.0:42000") < 0) {
        Manager::finalize();
        return;
    }

    // Only two buffer necessary
    std::vector<char> global_receive_buf(MSG_SIZE);
    std::vector<char> global_send_buf(MSG_SIZE);

    while(true) {
        auto h = Manager::getNext();
        if(!h.isValid()){
            Manager::finalize();
            return;
        }

        size_t client_id = h.getID();

        if(h.isNewConnection()){
            active_clients.try_emplace(client_id, ClientSession{client_id});
            continue;
        }

        auto it = active_clients.find(client_id);
        if(it == active_clients.end()) continue;

        size_t inc_size = 0;
        ssize_t pr = h.probe(inc_size, false);

        if(pr > 0 && inc_size > 0){
            // 2. Assicuriamoci che i buffer globali siano grandi abbastanza
            if (global_receive_buf.size() < inc_size)
                global_receive_buf.resize(inc_size);
            if (global_send_buf.size() < inc_size)
                global_send_buf.resize(inc_size);

            // 3. Riceviamo nel buffer globale
            ssize_t bytes_read = h.receive(global_receive_buf.data(), inc_size);

            if(bytes_read > 0){
                std::transform(std::execution::unseq,
                               global_receive_buf.begin(),
                               global_receive_buf.begin() + bytes_read,
                               global_send_buf.begin(),
                               [](char c){return c + 1;});

                // 4. Inviamo dal buffer globale
                h.send(global_send_buf.data(), bytes_read);
            }
        } else if (pr == 0 || h.isClosed().first) {
            active_clients.erase(it);
        }
    }
}


void client(std::string endpoint) {
    auto h = Manager::connect(endpoint, 5, 500);
    if(!h.isValid()) {
        Manager::finalize();
        return;
    }


    auto t_start = std::chrono::high_resolution_clock::now();
    for(char c = 'A'; c<= 'Z'; ++c) {
        const size_t CHUNK_SIZE = 50 * 1024 * 1024; // 50 MB
        std::vector<char> send_buf(CHUNK_SIZE, c);
        std::vector<char> recv_buf(CHUNK_SIZE, 0);

        h.send(send_buf.data(), send_buf.size());

        ssize_t bytes_read = h.receive(recv_buf.data(), recv_buf.size());

        if (bytes_read <= 0) {
            std::cerr << "Errore o disconnessione durante la ricezione.\n";
        } else if (static_cast<size_t>(bytes_read) == CHUNK_SIZE) {
            bool success = std::all_of(std::execution::unseq,
                                       recv_buf.begin(), recv_buf.begin() + bytes_read,
                                       [expected = (char)(c+1)](char a) { return a == expected;});
            if (!success) {
                std::cerr << "[ERRORE] Il server ha restituito dati non validi o parzialmente non trasformati.\n";
            }
        } else {
            std::cerr << "[ATTENZIONE] Letti solo " << bytes_read << " byte su " << CHUNK_SIZE << ".\n";
        }
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = t_end - t_start;
    double seconds = diff.count();

    std::cout << "Test concluso, tempo totale: " << seconds << "\n";
    h.close();
}

int main(int argc, char** argv){
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <0 for server, 1 for client> [server_host] [num_threads]\n";
        return EXIT_FAILURE;
    }

    int myid = std::atoi(argv[1]);
    std::string server_host = (argc >= 3) ? argv[2] : "localhost";
    int num_threads = (argc >= 4) ? std::atoi(argv[3]) : 1;

    if (Manager::init(std::to_string(myid)) < 0) {
        std::cerr << "Error init MTCL\n";
        return EXIT_FAILURE;
    }

    if(myid == 0) {
        server();
    } else {
        std::vector<std::thread> threads;
        for(int i = 0; i < num_threads; ++i){
            threads.emplace_back(client, "TCP:" + server_host + ":42000");
        }

        for(auto &t : threads){
            if(t.joinable())
                t.join();
        }
    }

    Manager::finalize();
    return 0;
}
