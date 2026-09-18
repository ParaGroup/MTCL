/**
 * - Sender ships 10 payload
 * - Receiver creates 10 individual Requests and starts receiving the payloads
 * - After the first 5 messages, the requests get destroyed while a message was half read.
 * - The next receive requests made by the receiver should go well without problems (NO YIELD).
 * How to run:
 * Client: ./test_request_drain 1 [server_ip]
 * Server: ./test_request_drain 0
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

std::string ENDPOINT;
const size_t NUM_MESSAGES = 10;
const size_t PAYLOAD_SIZE = 50 * 1024 * 1024; // 50 MB

void server() {
    // SENDER
    if (Manager::listen("TCP:0.0.0.0:42000") < 0) {
        std::cerr << "[SENDER] Listen failed\n";
        return;
    }

    auto h = Manager::getNext(); // Accepting connection
    if (!h.isValid()) {
        std::cerr << "[SENDER] Accept failed\n";
        return;
    }

    std::cout << "[SENDER] Connected. Allocating 10 payloads of 50MB...\n";
    std::vector<std::vector<char>> send_bufs(NUM_MESSAGES, std::vector<char>(PAYLOAD_SIZE));

    for (size_t i = 0; i < NUM_MESSAGES; ++i) {
        std::fill(send_bufs[i].begin(), send_bufs[i].end(), 'A' + i);
    }

    std::cout << "[SENDER] Sending 10 payloads...\n";
    for (size_t i = 0; i < NUM_MESSAGES; ++i) {
        ssize_t sent = h.send(send_bufs[i].data(), PAYLOAD_SIZE);
        if (sent <= 0) {
            std::cerr << "[SENDER] Error sending messages " << i << "\n";
            return;
        }
        std::cout << "[SENDER] Sent message " << i << " ('" << (char)('A' + i) << "')\n";
    }

    std::cout << "[SENDER] All 10 messages successfully sent.\n";
    h.close();
}

/*Instantiate 10 ireceive but interrupts the 6th mid flight to check drainage system*/
void receive_messages(HandleUser& h, std::vector<std::vector<char>>& recv_bufs) {

    std::vector<Request> reqs(NUM_MESSAGES);

    for (size_t i = 0; i < NUM_MESSAGES; ++i) {
        h.ireceive(recv_bufs[i].data(), PAYLOAD_SIZE, reqs[i]);
    }

    reqs[4].wait();

    // When the 6th request starts receiving data, it gets destroyied and it's data must be drained
    while (reqs[5].count() == 0) {
        test(reqs[5]);
        if(reqs[5].count()==PAYLOAD_SIZE){
            std::cout<<"Too far, message fully read\n";
            return;
        }
    }

    std::cout << "\n[RECEIVER] >>> Destroing request mid flight! <<<\n\n";
}

void client(std::string server_host) {
    // RECEIVER
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ENDPOINT = "TCP:" + server_host + ":42000";
    auto h = Manager::connect(ENDPOINT);
    if (!h.isValid()) {
        std::cerr << "[RECEIVER] Connect failed\n";
        return;
    }

    std::vector<std::vector<char>> recv_bufs(NUM_MESSAGES, std::vector<char>(PAYLOAD_SIZE, 0));

    // Receive first 5 messages and interrupt the last one mid flight
    receive_messages(h, recv_bufs);

    std::cout << "[RECEIVER] Posting 4 new ireceive for the remaining messages...\n";

    std::vector<Request> recovery_reqs(4);
    for (size_t i = 6; i < NUM_MESSAGES; ++i) {
        h.ireceive(recv_bufs[i].data(), PAYLOAD_SIZE, recovery_reqs[i - 6]);
    }

    if(recovery_reqs[3].wait() != 0){
        std::cout<<"Error receiving messages\n";
        exit(-1);
    }

    std::cout << "[RECEIVER] All the remaining messages received!\n";

    bool success = true;
    for (size_t i = 0; i < 5; ++i) { // Check first 5 messages
        if (recv_bufs[i][0] != 'A' + i) {
            std::cerr << "Error corruption in MSG " << i << "\n";
            success = false;
        }
    }

    // Message 5 has been interrupt and drained, no need to check it.
    for (size_t i = 6; i < NUM_MESSAGES; ++i) { // Check last 4 messages
        if (recv_bufs[i][0] != 'A' + i) {
            std::cerr << "Error! Corruption in message " << i << ". Expected " << (char)('A'+i) << " but found " << recv_bufs[i][0] << "\n";
            success = false;
        }
    }

    if (success) {
        std::cout << "\n[RECEIVER] Success! No corruption, DRAIN completed.\n";
    } else {
        std::cerr << "\n[RECEIVER] TEST failed!\n";
    }

    h.close();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <0 for sender, 1 for receiver>\n";
        return -1;
    }

    int user = atoi(argv[1]);
    std::string server_host = (argc >= 3) ? argv[2] : "localhost";

    if (Manager::init(std::to_string(user)) < 0) {
        std::cerr << "Error init MTCL\n";
        return -1;
    }

    if (user == 0) {
        server();
    } else {
        client(server_host);
    }

    Manager::finalize();
    return 0;
}
