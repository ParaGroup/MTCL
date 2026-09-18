/**
 * Test to validate the creation and destruction of requests.
 * Server sends 2000 payloads of 1KB each;
 * Client create a Request for each message and destroy it after validating
 * the correctness of the payload.
 * How to run:
 *   Server: ./test_leak 0
 *   Client: ./test_leak 1 [server_ip]
 */

#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <iomanip>
#include <cstdlib>

#include "mtcl.hpp"

using namespace MTCL;

static constexpr int TEST_PORT = 45000;
static constexpr int NUM_MESSAGES = 2000;
static constexpr size_t MSG_SIZE = 1024; // 1 KB per message

/* Server sends 2000 messages(1KB each) */
static void test_server(int port) {
    std::string ep = "TCP:localhost:" + std::to_string(port);

    if (Manager::listen(ep) < 0) {
        std::cerr << "[SERVER] ERROR: listen failed\n";
        exit(EXIT_FAILURE);
    }

    std::cout << "[SERVER] Listening on " << ep << ", waiting for client...\n";
    auto h = Manager::getNext();

    if (!h.isValid()) {
        std::cerr << "[SERVER] ERROR: Not a valid Handle.\n";
        exit(EXIT_FAILURE);
    }

    std::cout << "[SERVER] Sending " << NUM_MESSAGES << " messages...\n";

    std::vector<char> send_buf(MSG_SIZE);

    for (int i = 0; i < NUM_MESSAGES; ++i) {
        // Fill the buffer so that the client can check the validity
        std::fill(send_buf.begin(), send_buf.end(), static_cast<char>(i % 256));

        ssize_t sent = h.send(send_buf.data(), send_buf.size());

        if (sent != static_cast<ssize_t>(send_buf.size())) {
            std::cerr << "[SERVER] ERROR: Send " << i << " failed.\n";
            exit(EXIT_FAILURE);
        }

        // Print progress every 500 messages
        if ((i + 1) % 500 == 0) {
            std::cout << "[SERVER] Sent " << (i + 1) << "/" << NUM_MESSAGES << " messagges so far.\n";
        }
    }

    std::cout << "[SERVER] Sending completed.\n";
    h.close();
}

/* Each request is created and elaborated here. */
static void receive_and_verify_single_message(HandleUser& h, int expected_index) {
    std::vector<char> recv_buf(MSG_SIZE, 0);

    Request req;

    if (h.ireceive(recv_buf.data(), recv_buf.size(), req) < 0) {
        std::cerr << "[CLIENT] ERROR: ireceive failed at index: " << expected_index << ".\n";
        exit(EXIT_FAILURE);
    }

    if (req.wait() < 0) {
        std::cerr << "[CLIENT] ERROR: wait() failed at index: " << expected_index << ".\n";
        exit(EXIT_FAILURE);
    }

    // Verifica dell'integrità del payload
    char expected_char = static_cast<char>(expected_index % 256);
    if (recv_buf[0] != expected_char) {
        std::cerr << "[CLIENT] DATA ERROR: Messagge " << expected_index
                  << " corrupted. Expected: " << (int)expected_char
                  << ", Received: " << (int)recv_buf[0] << "\n";
        exit(EXIT_FAILURE);
    }
    // here every req gets destructed
}

static void test_client(int port) {
    std::string ep = "TCP:localhost:" + std::to_string(port);

    std::cout << "[CLIENT] Connecting to " << ep << "...\n";
    auto h = Manager::connect(ep);

    if (!h.isValid()) {
        std::cerr << "[CLIENT] ERROR: Can't establish a connection with the server.\n";
        exit(EXIT_FAILURE);
    }

    std::cout << "[CLIENT] Receiving " << NUM_MESSAGES << " messagges...\n";

    for (int i = 0; i < NUM_MESSAGES; ++i) {
        // Create and destroy a request for each message
        receive_and_verify_single_message(h, i);

        if ((i + 1) % 500 == 0) {
            std::cout << "[CLIENT] Received and validated " << (i + 1) << "/" << NUM_MESSAGES << " messagges.\n";
        }
    }

    std::cout << "[CLIENT] Test completed, no leaks!\n";
    h.close();
}

int main(int argc, char** argv) {
    std::cout << std::unitbuf;

    if (argc < 2) {
        std::cerr << "Usage Server: " << argv[0] << " 0\n";
        std::cerr << "Usage Client: " << argv[0] << " 1 <IP_SERVER>\n";
        return 1;
    }

    int rank = std::atoi(argv[1]);

    if (rank == 0) {
        std::cout << "========== Starting TEST POOL LEAK (SERVER) ==========\n";
        if (Manager::init("server_pool_test") < 0) return 1;
        test_server(TEST_PORT);
        Manager::finalize();

    } else if (rank == 1) {

        std::cout << "========== Starting TEST POOL LEAK (CLIENT) ==========\n";
        if (Manager::init("client_pool_test") < 0) return 1;
        test_client(TEST_PORT);
        Manager::finalize();

    } else {
        std::cerr << "Invalid rank.\n";
        return 1;
    }
    return 0;
}
