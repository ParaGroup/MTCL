/**
 * Async drain test for ireceive
 *
 * Goal:
 *  - Sender sends an oversized message M1 (BIG bytes) followed by a small 
 *    message M2 ("OK").
 *  - Receiver posts an async ireceive with a too-small buffer for M1.
 *    Expect: request.wait() returns -1 and errno == EMSGSIZE.
 *    The backend must drain the whole M1 to keep the stream aligned.
 *  - Receiver then posts a second ireceive for M2.
 *    Expect: success and correct payload ("OK").
 *
 * How to run:
 *   ./test_async_drain_stream TCP
 *   ./test_async_drain_stream UCX
 *
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

constexpr int DEFAULT_PORT = 42000;
const std::string DEFAULT_LABEL = "listen_label";

static std::string make_listen(const std::string& proto, int port) {
    if (proto == "TCP") {
        return "TCP:localhost:" + std::to_string(port);
    }
    if (proto == "UCX") {
        return "UCX:localhost:" + std::to_string(port);
    }
	if (proto == "MQTT") {
		return "MQTT:" + DEFAULT_LABEL;
	}
    return "";
}

static std::string make_connect(const std::string& proto, int port) {
    if (proto == "TCP") {
        return "TCP:localhost:" + std::to_string(port);
    }
    if (proto == "UCX") {
        return "UCX:localhost:" + std::to_string(port);
    }
	if (proto == "MQTT") {
		return "MQTT:" + DEFAULT_LABEL;
	}
    return "";
}

static int listen_with_retry(const std::string& ep, int max_tries = 20) {
    for (int i = 0; i < max_tries; ++i) {
        if (Manager::listen(ep) == 0) return 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return -1;
}

static HandleUser connect_with_retry(const std::string& ep, int max_tries = 50) {
	return Manager::connect(ep, max_tries, 100);
}

static int server_proc(const std::string& proto, int port, size_t big_size) {
    const std::string listen_ep = make_listen(proto, port);
    if (listen_ep.empty()) {
        std::cerr << "Unsupported proto: " << proto << "\n";
        return 2;
    }

    Manager::init("test");

    if (listen_with_retry(listen_ep) != 0) {
        std::cerr << "Server: listen failed on " << listen_ep << " errno=" << errno
                  << " (" << std::strerror(errno) << ")\n";
        Manager::finalize();
        return 3;
    }

	std::cout << "Server listenin on " << listen_ep << "\n";
	
    // Accept one connection
    auto h = Manager::getNext();
    if (!h.isValid() || !h.isNewConnection()) {
        std::cerr << "Server: getNext() did not return a new valid connection\n";
        if (h.isValid()) h.close();
        Manager::finalize();
        return 4;
    }

    // Oversize async receive: expect EMSGSIZE, and stream must remain aligned
    const size_t small_cap = 64;
    std::vector<char> small_buf(small_cap);

    Request r1;
    errno = 0;
    if (h.ireceive(small_buf.data(), small_buf.size(), r1) < 0) {
        std::cerr << "Server: ireceive(M1) failed immediately errno=" << errno
                  << " (" << std::strerror(errno) << ")\n";
        h.close();
        Manager::finalize();
        return 5;
    }

    if (r1.wait() == 0) {
        std::cerr << "Server: expected EMSGSIZE for M1, but wait() succeeded.\n";
        std::cerr << "Server: r1.count()=" << r1.count() << "\n";
        h.close();
        Manager::finalize();
        return 6;
    }

    if (errno != EMSGSIZE) {
        std::cerr << "Server: expected errno=EMSGSIZE, got errno=" << errno
                  << " (" << std::strerror(errno) << ")\n";
        std::cerr << "Server: r1.count()=" << r1.count() << "\n";
        h.close();
        Manager::finalize();
        return 7;
    }

    const ssize_t drained_size = r1.count();
    if (drained_size != (ssize_t)big_size) {
        std::cerr << "Server: expected r1.count()==" << big_size
                  << ", got " << drained_size << "\n";
        h.close();
        Manager::finalize();
        return 8;
    }

    // Receive next message 
    char ok_buf[3] = {0, 0, 0};
    Request r2;
    errno = 0;
    if (h.ireceive(ok_buf, 3, r2) < 0) {
        std::cerr << "Server: ireceive(M2) failed immediately errno=" << errno
                  << " (" << std::strerror(errno) << ")\n";
        h.close();
        Manager::finalize();
        return 9;
    }

    if (r2.wait() != 0) {
        std::cerr << "Server: M2 wait() failed errno=" << errno
                  << " (" << std::strerror(errno) << ")\n";
        h.close();
        Manager::finalize();
        return 10;
    }
	if (r2.count() != 2) {
        std::cerr << "Server: M2 count mismatch, got " << r2.count() << " expected 2\n";
        h.close();
        Manager::finalize();
        return 11;
	}
	
    if (std::string(ok_buf, ok_buf + 2) != "OK") {
        std::cerr << "Server: M2 payload mismatch, got \"" << std::string(ok_buf, 2) << "\"\n";
        h.close();
        Manager::finalize();
        return 11;
    }

    h.close();
    Manager::finalize();
    return 0;
}

static int client_proc(const std::string& proto, int port, size_t big_size) {
    const std::string connect_ep = make_connect(proto, port);
    if (connect_ep.empty()) {
        std::cerr << "Unsupported proto: " << proto << "\n";
        return 2;
    }

    Manager::init("test");

	auto h = connect_with_retry(connect_ep);
	if (!h.isValid()) {
        std::cerr << "Client: connect failed to " << connect_ep << " errno=" << errno
                  << " (" << std::strerror(errno) << ")\n";
        Manager::finalize();
        return 3;
    }

	std::cout << "Client connected to " << connect_ep << "\n";

    // Send BIG then OK
    std::vector<char> big(big_size, 'A');
    if (h.send(big.data(), big.size()) < 0) {
        std::cerr << "Client: send(M1) failed errno=" << errno
                  << " (" << std::strerror(errno) << ")\n";
        h.close();
        Manager::finalize();
        return 5;
    }

    const char ok[] = "OK";
    if (h.send(ok, 2) < 0) {
        std::cerr << "Client: send(M2) failed errno=" << errno
                  << " (" << std::strerror(errno) << ")\n";
        h.close();
        Manager::finalize();
        return 6;
    }

    h.close();
    Manager::finalize();
    return 0;
}

int main(int argc, char** argv) {
    std::string proto = "TCP";
    if (argc > 1) proto = argv[1];

    // keep BIG moderate so the test doesn't take too long; still large enough to overflow small buffer
    const size_t BIG = (argc > 2) ? (size_t)std::stoul(argv[2]) : (256 * 1024); // 256 KiB

    pid_t pid = fork();
    if (pid < 0) {
        std::cerr << "fork() failed\n";
        return 2;
    }

    if (pid == 0) { // child
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        int rc = client_proc(proto, DEFAULT_PORT, BIG);
        _exit(rc);
    }

    // parent: server
    int srv_rc = server_proc(proto, DEFAULT_PORT, BIG);

    int status = 0;
    waitpid(pid, &status, 0);

    if (!WIFEXITED(status)) {
        std::cerr << "Client did not exit cleanly\n";
        return 20;
    }
    int cli_rc = WEXITSTATUS(status);

    if (srv_rc != 0) {
        std::cerr << "Server failed with rc=" << srv_rc << "\n";
        return srv_rc;
    }
    if (cli_rc != 0) {
        std::cerr << "Client failed with rc=" << cli_rc << "\n";
        return cli_rc;
    }

    std::cout << "Test passed (proto=" << proto << ")\n";
    return 0;
}
