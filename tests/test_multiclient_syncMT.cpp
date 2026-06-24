/**
 * Client-Server model, each client sends 26 messages (one for each letter of the
 * alphabet). The server receive the payload, elaborate it and sends it back.
 * This version of the server is Synchronous with one thread for each request.
 * How to run:
 *   Server: ./test_multiclient_syncMT 0
 *   Client: ./test_multiclient_syncMT 1 [server_host] [threads_num]
 */



#include <cerrno>
#include <csignal>
#include <cstring>
#include <iostream>
#include <string>
#include <future>
#include <thread>
#include <vector>
#include <chrono>

#include <sys/wait.h>
#include <unistd.h>
#include <execution>

#include "mtcl.hpp"

using namespace MTCL;

HandleUser asyncTask(HandleUser h){
    size_t inc_size = 0;
    ssize_t pr = h.probe(inc_size, false);

    if(pr > 0 && inc_size > 0){

        std::vector<char> buf(inc_size);

        ssize_t bytes_read = h.receive(buf.data(), inc_size);

        if(bytes_read > 0){
            std::transform(std::execution::unseq,
                           buf.begin(),
                           buf.begin() + bytes_read,
                           buf.begin(),
                           [](char c){return c + 1;});

            h.send(buf.data(), bytes_read);
        }
        return std::move(h);
    } else if (pr == 0 || h.isClosed().first) {
        return MTCL::HandleUser();
    }
}

void server() {
    if (Manager::listen("TCP:0.0.0.0:42000") < 0) {
        Manager::finalize();
        return;
    }

    std::vector<std::future<HandleUser>> futures;
    std::deque<HandleUser> waiting_clients;
    int size_limit = 5;

    while(true) {
        auto h = Manager::getNext(std::chrono::microseconds(500));
        if(!h.isValid()) {
            auto it = futures.begin();
            while(it != futures.end()) {
                if(it->wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
                    HandleUser result = it->get();
                    if(result.isValid())
                        result.yield();
                    it = futures.erase(it);
                }else {
                    ++it;
                }
                while(futures.size() < size_limit && !waiting_clients.empty()) {
                    HandleUser queued_h = std::move(waiting_clients.front());
                    waiting_clients.pop_front();
                    futures.push_back(std::async(std::launch::async, asyncTask, std::move(queued_h)));
                }
            }
        } else {
            size_t client_id = h.getID();

            if(h.isNewConnection()){
                continue;
            }
            if(futures.size() >= size_limit){
                waiting_clients.push_back(std::move(h));
            } else {
                futures.push_back(std::async(std::launch::async, asyncTask, std::move(h)));
            }
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
