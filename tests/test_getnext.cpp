/**
 * getnext test
 * Goal:
 *  - Sender sends two messages
 *  - Receiver posts an async ireceive followed by a yield.
 *  - Receiver calls getNext, and update should return the handle to the receiver after
 *  -  completing the first ireceive because a new payload is coming.
 *  - Now receiver can post a new Receive for the second message.
 *    Expected: Success.
 */


#include <cerrno>
#include <csignal>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include "mtcl.hpp"

using namespace MTCL;

const std::string ENDPOINT = "TCP:localhost:42000";
const size_t PAYLOAD_SIZE = 50 * 1024 * 1024;

void server() {
    if (Manager::listen(ENDPOINT) < 0) {
        std::cerr << "[SERVER] Listen failed\n";
        return;
    }

    std::cout << "[SERVER] Waiting the client...\n";
    auto h = Manager::getNext();
    if (!h.isValid()) {
        std::cerr << "[SERVER] Accept failed\n";
        return;
    }

    std::vector<char> send_buf1(PAYLOAD_SIZE, 'A');
    std::vector<char> send_buf2(PAYLOAD_SIZE, 'B');

    Request req1, req2;

    std::cout << "[SERVER] Starting two isend...\n";
    if (h.isend(send_buf1.data(), PAYLOAD_SIZE, req1) < 0 ||
        h.isend(send_buf2.data(), PAYLOAD_SIZE, req2) < 0) {
        std::cerr << "[SERVER] isend error\n";
        return;
    }

    std::cout << "[SERVER] Waiting isend completion...\n";
    req1.wait();
    req2.wait();

    std::cout << "[SERVER] isend completed.\n";
    h.close();
}

void client() {
    std::this_thread::sleep_for(std::chrono::seconds(1));

    auto h = Manager::connect(ENDPOINT);
    if (!h.isValid()) {
        std::cerr << "[CLIENT] Connect failed\n";
        return;
    }

    std::vector<char> recv_buf1(PAYLOAD_SIZE, 0);
    std::vector<char> recv_buf2(PAYLOAD_SIZE, 0);

    Request req1;

    std::cout << "[CLIENT] Starting ireceive for the first payload...\n";
    if (h.ireceive(recv_buf1.data(), PAYLOAD_SIZE, req1) < 0) {
        std::cerr << "[CLIENT] ireceive error\n";
        return;
    }

    std::cout << "[CLIENT] Yielding...\n";
    h.yield();

    std::cout << "[CLIENT] Waiting for handle...\n";
    auto h_returned = Manager::getNext();
    if (!h_returned.isValid()) {
        std::cerr << "[CLIENT] getnext error\n";
        return;
    }

    std::cout << "[CLIENT] Got a handle, server is sending new data...\n";

    ssize_t recvd = h_returned.receive(recv_buf2.data(), PAYLOAD_SIZE);
    if (recvd <= 0) {
        std::cerr << "[CLIENT] receive error\n";
        return;
    }

    std::cout << "[CLIENT] Got second payload (" << recvd / (1024*1024) << " MB).\n";

    req1.wait();

    for (size_t i = 0; i < PAYLOAD_SIZE; ++i) {
        if (recv_buf1[i] != 'A' || recv_buf2[i] != 'B') {
            std::cerr << "[CLIENT] Payload corrupted\n";
            return;
        }
    }

    std::cout << "[CLIENT] Success.\n";
    h_returned.close();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <0 for server, 1 for client>\n";
        return -1;
    }

    int user = atoi(argv[1]);

    if (Manager::init(std::to_string(user)) < 0) {
        std::cerr << "Error init MTCL\n";
        return -1;
    }

    if (user == 0) {
        server();
    } else {
        client();
    }

    Manager::finalize();
    return 0;
}
