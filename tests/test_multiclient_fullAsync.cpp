/**
 * Client-Server model, each client sends 26 messages (one for each letter of the
 * alphabet). The server receive the payload, elaborate it and sends it back.
 * This version of the server is Asynchronous, with a limit of 40 request handled
 * concurrently.
 * How to run:
 *   Server: ./test_multiclient_fullAsync 0
 *   Client: ./test_multiclient_fullAsync 1 [server_host] [threads_num]
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

int MSG_SIZE = 50*1024*1024;
int REQ_LIMIT = 40;

struct RequestHandler {
    Request req;
    size_t client_id;
    HandleUser h;
    std::vector<char> buf;
    bool is_receive;
    bool is_free;

    RequestHandler() {
        is_free = true;
        buf.resize(MSG_SIZE);
    }

    void work_on_data() {
    std::transform(buf.begin(),
                   buf.end(),
                   buf.begin(),
                   [](char c) { return c + 1; });
    }
};

void server() {
    if (Manager::listen("TCP:0.0.0.0:42000") < 0) {
        Manager::finalize();
        return;
    }

    // one queue for started request, one for free req
    // and one for connections that needs to be Handled
    std::deque<RequestHandler> pending_req;
    std::deque<RequestHandler> free_req(REQ_LIMIT);
    std::deque<HandleUser> to_be_handled;

    // mapping all handles connected
    std::unordered_map<size_t, HandleUser> saved_handles;

    HandleUser h;

    while(true) {
        h = Manager::getNext(std::chrono::microseconds(500));

        // if the handle returned by the manager is not valid(getNext timeout)
        if (!h.isValid()) {
            for(auto client = pending_req.begin(); client != pending_req.end(); ){
                auto& c = *client;

                // check if request is completed
                if (test(c.req)) {

                    if (c.is_receive) { // ireceive request completed
                        // if connection is closed -> reuse request or set as free
                        if(c.req.count() == 0){
                            size_t dummy;
                            c.h.probe(dummy, false);
                            c.h.close();

                            if(!to_be_handled.empty()) {
                                c.h = std::move(to_be_handled.front());
                                to_be_handled.pop_front();

                                c.client_id = c.h.getID();
                                c.h.ireceive(c.buf.data(), MSG_SIZE, c.req);
                                c.h.yield();

                                ++client;
                                continue;
                            } else {
                                c.is_free = true;
                                free_req.push_back(std::move(c));
                                client = pending_req.erase(client);
                                continue;
                            }
                        }
                        // if ireceive is completed, make isend
                        c.work_on_data();
                        c.h.isend(c.buf.data(), MSG_SIZE, c.req);
                        c.is_receive = false;
                    } else { // isend request completed

                        size_t old_id = c.client_id;
                        saved_handles[old_id] = std::move(c.h);

                        if (to_be_handled.empty()) {
                            c.is_free = true;
                            free_req.push_back(std::move(c));
                            client = pending_req.erase(client);
                            continue;
                        } else {
                            c.h = std::move(to_be_handled.front());
                            to_be_handled.pop_front();
                            c.client_id = c.h.getID();
                            c.is_receive = true;
                            c.h.ireceive(c.buf.data(), MSG_SIZE, c.req);
                            c.h.yield();
                        }
                    }
                }
                ++client;
            }
        } else {// manager returns a valid handle
            auto new_c = h.isNewConnection();

            if(new_c){
                size_t id = h.getID();
                saved_handles.try_emplace(id, std::move(h));
                if(auto hndl = saved_handles.find(id); hndl != saved_handles.end())
                    hndl->second.yield();
            } else {
                size_t id = h.getID();
                saved_handles.erase(id);
                if (free_req.empty()) {
                    // save handle for when there is a request free
                    to_be_handled.push_back(std::move(h));
                } else {
                    pending_req.push_back(std::move(free_req.front()));
                    free_req.pop_front();

                    auto& stable_req = pending_req.back();

                    stable_req.h = std::move(h);
                    stable_req.client_id = id;
                    stable_req.is_receive = true;
                    stable_req.is_free = false;

                    stable_req.h.ireceive(stable_req.buf.data(), MSG_SIZE, stable_req.req);
                    stable_req.h.yield();
                }
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
        const size_t CHUNK_SIZE = 50 * 1024 * 1024;
        std::vector<char> send_buf(CHUNK_SIZE, c);
        std::vector<char> recv_buf(CHUNK_SIZE, 0);

        h.send(send_buf.data(), send_buf.size());

        ssize_t bytes_read = h.receive(recv_buf.data(), recv_buf.size());

        if (bytes_read <= 0) {
            std::cerr << "Errore o disconnessione durante la ricezione. " << c<< "\n";
        } else if (static_cast<size_t>(bytes_read) == CHUNK_SIZE) {
            bool success = std::all_of(std::execution::unseq,
                                       recv_buf.begin(), recv_buf.begin() + bytes_read,
                                       [expected = (char)(c+1)](char a) { return a == expected;});
            if (success) {
                //std::cout << "[OK] \n";
            } else {
                std::cerr << "[ERRORE] Il server ha restituito dati non validi o parzialmente non trasformati.\n";
            }
        } else {
            std::cerr << "[ATTENZIONE] Letti solo " << bytes_read << " byte su " << CHUNK_SIZE << ".\n";
        }
        //std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = t_end - t_start;
    double seconds = diff.count();

    std::cout << "Test concluso, tempo totale: " << seconds << ". Chiusura connessione.\n";
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
