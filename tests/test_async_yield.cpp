/*
 * Test for MTCL library: Validates single and concurrent I/O operations also using yield logic.
 * The server and client exchange large buffers (150 MB) and verify data
 * integrity after transmission.
 */
#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <vector>
#include <iomanip>

#include "mtcl.hpp"

using namespace MTCL;

static constexpr int DEFAULT_PORT = 42000;
// 150 MB
static constexpr ssize_t PAYLOAD_SIZE = 150 * 1024 * 1024;
// 10 MB for testing RequestPool
static constexpr ssize_t SMALL_PAYLOAD_SIZE = 10 * 1024 * 1024;
static constexpr int POOL_SIZE = 5;

void fill_buffer(std::vector<char>& buf, int rank) {
    for (size_t i = 0; i < buf.size(); ++i) {
        buf[i] = static_cast<char>((i + rank) % 256);
    }
}

bool verify_buffer(const std::vector<char>& buf, int expected_rank) {
    for (size_t i = 0; i < buf.size(); ++i) {
        if (buf[i] != static_cast<char>((i + expected_rank) % 256)) {
            return false;
        }
    }
    return true;
}

static int server() {
    std::string ep = "TCP:localhost:" + std::to_string(DEFAULT_PORT);

    if (Manager::listen(ep) < 0) {
        std::cerr << "[SERVER] Listen failed\n";
        return -1;
    }

    std::cout << "[SERVER] Waiting for client...\n";
    auto h = Manager::getNext();
    if(!h.isValid()) return -1;

    // Single and Concurrent I/O (150 MB)
    std::vector<char> send_buf(PAYLOAD_SIZE);
    std::vector<char> recv_buf(PAYLOAD_SIZE, 0);

    fill_buffer(send_buf, 0);

    Request req_recv, req_send;

    if (h.ireceive(recv_buf.data(), PAYLOAD_SIZE, req_recv) < 0 ||
        h.isend(send_buf.data(), PAYLOAD_SIZE, req_send) < 0) {
        std::cerr << "[SERVER] Error starting I/O requests\n";
        return -1;
    }

    req_recv.wait();

    req_send.wait();

    std::cout << "\n[SERVER] Single and Concurrent I/O completed.\n";
    if (!verify_buffer(recv_buf, 1)) {
        std::cerr << "[SERVER] ERROR: Corrupted data!\n";
        return -1;
    }

    std::vector<char> send_buf2(PAYLOAD_SIZE);
    std::vector<char> recv_buf2(PAYLOAD_SIZE, 0);

    fill_buffer(send_buf2, 0);

    Request req_recv2, req_send2;

    if (h.ireceive(recv_buf2.data(), PAYLOAD_SIZE, req_recv2) < 0) {
        std::cerr << "[SERVER] Error starting I/O requests\n";
        return -1;
    }

    if(h.isend(send_buf2.data(), PAYLOAD_SIZE, req_send2) < 0) {
        std::cerr << "[SERVER] Error starting I/O requests\n";
        return -1;
    }


    h.yield();

    if((req_recv2.wait()<0) || (req_send2.wait()<0)){
        std::cerr << "[CLIENT] Error: Send or receive failed!\n";
        return -1;
    }


    std::cout << "\n[SERVER] Single and Concurrent I/O completed.\n";
    if (!verify_buffer(recv_buf2, 1)) {
        std::cerr << "[SERVER] ERROR: Corrupted data!\n";
        return -1;
    }

    h.close();
    return 0;
}

static int client() {
    std::string ep = "TCP:localhost:" + std::to_string(DEFAULT_PORT);

    std::this_thread::sleep_for(std::chrono::seconds(1));

    auto h = Manager::connect(ep);
    if (!h.isValid()) {
        std::cerr << "[CLIENT] Connect failed\n";
        return -1;
    }

    // Single and Concurrent I/O (150 MB)
    std::vector<char> send_buf(PAYLOAD_SIZE);
    std::vector<char> recv_buf(PAYLOAD_SIZE, 0);

    fill_buffer(send_buf, 1);

    Request req_recv, req_send;

    if (h.ireceive(recv_buf.data(), PAYLOAD_SIZE, req_recv) < 0 ||
        h.isend(send_buf.data(), PAYLOAD_SIZE, req_send) < 0) {
        std::cerr << "[CLIENT] Error starting I/O requests\n";
        return -1;
    }

    if((req_send.wait()<0) || (req_recv.wait()<0)){
        std::cerr << "[CLIENT] Error: Send or receive failed!\n";
        return -1;
    }

    std::cout << "\n[CLIENT] Single and Concurrent I/O completed.\n";
    if (!verify_buffer(recv_buf, 0)) {
        std::cerr << "[CLIENT] ERROR: Corrupted data!\n";
        return -1;
    }

    // Single and Concurrent I/O (150 MB)
    std::vector<char> send_buf2(PAYLOAD_SIZE);
    std::vector<char> recv_buf2(PAYLOAD_SIZE, 0);

    fill_buffer(send_buf2, 1);

    Request req_recv2, req_send2;

    int i = 0;
    if ((i = h.ireceive(recv_buf2.data(), PAYLOAD_SIZE, req_recv2)) < 0) {
        std::cerr << "[SERVER] Error starting I/O requests\n";
        return -1;
    }

    if(h.isend(send_buf2.data(), PAYLOAD_SIZE, req_send2) < 0) {
        std::cerr << "[SERVER] Error starting I/O requests\n";
        return -1;
    }

    h.yield();

    if((req_send2.wait()<0) || (req_recv2.wait()<0)){
        std::cerr << "[CLIENT] Error: Send or receive failed!\n";
        return -1;
    }

    std::cout << "\n[CLIENT] Single and Concurrent I/O completed.\n";
    if (!verify_buffer(recv_buf2, 0)) {
        std::cerr << "[CLIENT] ERROR: Corrupted data!\n";
        return -1;
    }

    h.close();
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 2) return -1;

    int rank = std::atoi(argv[1]);

    if (Manager::init(std::to_string(rank)) < 0) return -1;

    int ret = (rank == 0) ? server() : client();

    Manager::finalize();
    return ret;
}
