/*
 * Test: RequestPool with testAll() polling and arena recycling on mixed payloads.
 *
 * Compilation:
 *   make test_tcp_requestpool TPROTOCOL=TCP
 *
 * Execution:
 *   Server:  ./test_tcp_requestPool 0 [iterations] [pool_size]
 *   Client:  ./test_tcp_requestpool 1 [iterations] [pool_size]
 *
 * Parameters:
 *   argv[1]  rank          0 = server (sends), 1 = client (receives)
 *   argv[2]  iterations    How many times the pool is recycled (default: 500)
 *   argv[3]  pool_size     Number of requests in the pool / messages per iteration
 *                          (default: 3). Messages cycle between 5B, 26B, 256B (j % 3)
 *
 * Example:
 *   ./test_tcp_requestpool 0 200 5   → server, 200 iterations, 5 msg/iter = 1000 messages
 *   ./test_tcp_requestpool 1 200 5   → client, receives and verifies with testAll() polling
 */

#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <cassert>
#include <vector>

#include "mtcl.hpp"

using namespace MTCL;

static constexpr int DEFAULT_PORT = 42100;

static void fail_and_finalize(const char* msg) {
    std::cerr << "[TEST] ERROR: " << msg
              << ", errno=" << errno << " (" << std::strerror(errno) << ")\n";
    Manager::finalize();
    exit(-1);
}

static void server(int num_iterations, int pool_size) {
    std::string ep = "TCP:localhost:" + std::to_string(DEFAULT_PORT);
    if (Manager::listen(ep) < 0)
        fail_and_finalize("Server: listen failed");

    std::cout << "[SERVER] Listening, waiting for client...\n";
    auto h = Manager::getNext();
    if (!h.isValid())
        fail_and_finalize("Server: getNext invalid handle");

    // Messages of different sizes to stress the pool
    const std::string msg_short = "HELLO";                           // 5 bytes
    const std::string msg_medium = "REQUEST_POOL_TEST_MESSAGE!";     // 26 bytes
    const std::string msg_long(256, 'X');                            // 256 bytes

    // The pool is reused in each iteration
    RequestPool pool(pool_size);

    std::cout << "[SERVER] Starting send: " << num_iterations
              << " iterations, pool_size=" << pool_size << "\n";

    for (int i = 0; i < num_iterations; i++) {
        // Send pool_size messages per iteration, cycling through sizes
        for (int j = 0; j < pool_size; j++) {
            const std::string* msg;
            switch (j % 3) {
                case 0: msg = &msg_short;  break;
                case 1: msg = &msg_medium; break;
                case 2: msg = &msg_long;   break;
            }
            if (h.isend(msg->data(), msg->size(), pool) < 0)
                fail_and_finalize("Server: isend failed");
        }

        pool.waitAll();
        pool.reset(); // Recycle requests in PrivateArena

        if (i == 0 || (i + 1) % 100 == 0) {
            std::cout << "[SERVER] iter " << (i + 1) << "/" << num_iterations
                      << " sent\n";
        }
    }

    h.close();
    std::cout << "[SERVER] Completed.\n";
}

static void client(int num_iterations, int pool_size) {
    std::string ep = "TCP:localhost:" + std::to_string(DEFAULT_PORT);
    auto h = Manager::connect(ep);
    if (!h.isValid())
        fail_and_finalize("Client: connect failed");

    const std::string msg_short = "HELLO";
    const std::string msg_medium = "REQUEST_POOL_TEST_MESSAGE!";
    const std::string msg_long(256, 'X');

    // Buffer for each request in the pool
    std::vector<std::vector<char>> buffers(pool_size);
    for (int j = 0; j < pool_size; j++) {
        buffers[j].resize(512, 0); // Large enough for all messages
    }

    RequestPool rp(pool_size);

    std::cout << "[CLIENT] Starting receive: " << num_iterations
              << " iterations, pool_size=" << pool_size << "\n";

    size_t total_polls = 0;

    for (int i = 0; i < num_iterations; i++) {
        // Clear buffers
        for (int j = 0; j < pool_size; j++) {
            std::fill(buffers[j].begin(), buffers[j].end(), 0);
        }

        // Post all ireceive
        for (int j = 0; j < pool_size; j++) {
            size_t expected_size;
            switch (j % 3) {
                case 0: expected_size = msg_short.size();  break;
                case 1: expected_size = msg_medium.size(); break;
                case 2: expected_size = msg_long.size();   break;
            }
            if (h.ireceive(buffers[j].data(), expected_size, rp) < 0)
                fail_and_finalize("Client: ireceive failed");
        }

        // Polling with testAll() until completion
        size_t polls = 0;
        while (!rp.testAll()) {
            polls++;
            // Simulate useful work between polls
            if (polls % 10000 == 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        }
        total_polls += polls;

        // Verify integrity of received messages
        for (int j = 0; j < pool_size; j++) {
            const std::string* expected;
            switch (j % 3) {
                case 0: expected = &msg_short;  break;
                case 1: expected = &msg_medium; break;
                case 2: expected = &msg_long;   break;
            }

            std::string received(buffers[j].data(), expected->size());
            if (received != *expected) {
                std::cerr << "[CLIENT] ERROR iter " << i << " msg " << j << "\n"
                          << "  expected:   \"" << expected->substr(0, 40) << "...\"\n"
                          << "  received:   \"" << received.substr(0, 40) << "...\"\n";
                h.close();
                Manager::finalize();
                exit(1);
            }
        }

        // Recycle requests for the next iteration
        rp.reset();

        if (i == 0 || (i + 1) % 100 == 0) {
            std::cout << "[CLIENT] iter " << (i + 1) << "/" << num_iterations
                      << " ok (polls: " << polls << ")\n";
        }
    }

    h.close();

    double avg_polls = (double)total_polls / num_iterations;
    std::cout << "[CLIENT] Completed. Average polls per iteration: "
              << avg_polls << "\n";
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0]
                  << " <0=server, 1=client> [iterations=500] [pool_size=3]\n";
        return 1;
    }

    int rank = std::atoi(argv[1]);
    int num_iterations = (argc > 2) ? std::atoi(argv[2]) : 500;
    int pool_size = (argc > 3) ? std::atoi(argv[3]) : 3;

    if (num_iterations <= 0) num_iterations = 500;
    if (pool_size <= 0) pool_size = 3;

    if (Manager::init("test_rp_" + std::to_string(rank)) < 0)
        fail_and_finalize("Manager::init failed");

    if (rank == 0) {
        server(num_iterations, pool_size);
    } else {
        client(num_iterations, pool_size);
    }

    Manager::finalize();
    return 0;
}
