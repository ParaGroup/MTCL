/**
 * - Sender ships 10 payloads
 * - Receiver creates a RequestPool and starts receiving the payloads
 * - After a few testAll, the pool gets destroyed.
 * - The next receive requests made by the receiver should work without problems.
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
const size_t NUM_MESSAGES = 10;
const size_t PAYLOAD_SIZE = 50 * 1024 * 1024; // 50 MB

void server() {
    // THE SENDER

    if (Manager::listen(ENDPOINT) < 0) {
        std::cerr << "[SENDER] Listen failed\n";
        return;
    }

    auto h = Manager::getNext();
    if (!h.isValid()) {
        std::cerr << "[SENDER] Accept failed\n";
        return;
    }

    std::cout << "[SENDER] Connected. Allocating 10 payloads of 50MB...\n";
    std::vector<std::vector<char>> send_bufs(NUM_MESSAGES, std::vector<char>(PAYLOAD_SIZE));

    for (size_t i = 0; i < NUM_MESSAGES; ++i) {
        std::fill(send_bufs[i].begin(), send_bufs[i].end(), 'A' + i);
    }

    for (size_t i = 0; i < NUM_MESSAGES; ++i) {
        ssize_t sent = h.send(send_bufs[i].data(), PAYLOAD_SIZE);
        if (sent <= 0) {
            std::cerr << "[SENDER] Error sending message " << i << " (Ignored, continuing...)\n";
            continue;
        }
        std::cout << "[SENDER] Sent message " << i << " ('" << (char)('A' + i) << "')\n";
    }

    std::cout << "[SENDER] All 10 messages processed. Closing.\n";
    h.close();
}

void receive_messages(HandleUser& h, std::vector<std::vector<char>>& recv_bufs) {
    RequestPool pool(10);

    for (size_t i = 0; i < NUM_MESSAGES; ++i) {
        h.ireceive(recv_bufs[i].data(), PAYLOAD_SIZE, pool);
    }

    std::cout << "[RECEIVER] Doing a few testAll() to pump the first data...\n";

    std::this_thread::sleep_for(std::chrono::seconds(1));

    pool.testAll();
    pool.testAll();
    pool.testAll();
    pool.testAll();
    pool.testAll();
    pool.testAll();
    pool.testAll();
    pool.testAll();

    std::this_thread::sleep_for(std::chrono::seconds(1));

    bool success = true;
    for (size_t i = 0; i < NUM_MESSAGES; ++i) {
        char c = recv_bufs[i][0];

        // Print what actually arrived (if it's 0, it means empty buffer)
        std::cout << "Buffer [" << i << "] starts with: " << (c == 0 ? "[EMPTY]" : std::string(1, c)) << "\n";

    }

    std::cout << "\n[RECEIVER] >>> DESTROYING REQUEST POOL MID-FLIGHT! <<<\n\n";
}

void client() {
    // THE RECEIVER

    auto h = Manager::connect(ENDPOINT);
    if (!h.isValid()) {
        std::cerr << "[RECEIVER] Connect failed\n";
        return;
    }

    std::vector<std::vector<char>> recv_bufs(NUM_MESSAGES, std::vector<char>(PAYLOAD_SIZE, 0));

    // Start receiving and destroy the RequestPool
    receive_messages(h, recv_bufs);

    // Resume directly for the remaining messages
    std::cout << "[RECEIVER] Creating new RequestPool for remaining messages...\n";

    std::vector<std::vector<char>> recover_bufs(NUM_MESSAGES, std::vector<char>(PAYLOAD_SIZE, 0));

    RequestPool pool_recovery(NUM_MESSAGES);
    for (size_t i = 0; i < NUM_MESSAGES; ++i) {
        h.ireceive(recover_bufs[i].data(), PAYLOAD_SIZE, pool_recovery);
    }

    pool_recovery.waitAll();

    std::cout << "\n[RECEIVER] --- PRINTING RECOVERED MESSAGES ---\n";

    for (size_t i = 0; i < NUM_MESSAGES; ++i) {
        char c = recover_bufs[i][0];

        // Print what actually arrived (if it's 0, it means empty buffer)
        std::cout << "Buffer [" << i << "] starts with: " << (c == 0 ? "[EMPTY]" : std::string(1, c)) << "\n";
    }
    h.close();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <0 for sender, 1 for receiver>\n";
        return -1;
    }

    int user = atoi(argv[1]);

    if (Manager::init(std::to_string(user)) < 0) {
        std::cerr << "MTCL init error\n";
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
