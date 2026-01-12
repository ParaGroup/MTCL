/*
 * EMSGSIZE receive retry test
 *
 * - The client sends a message bigger than the receiver buffer
 * - The server calls receive() with too-small buffer
 *   -> expect: return -1 and errno == EMSGSIZE, message must NOT be consumed
 * - The server reallocates a big enough buffer and calls receive() again
 *   -> expect: success and correct payload
 *
 * Build example:
 *   make clean TPROTOCOL="TCP|UCX|MQTT" test_emsgsize_receive.cpp
 *
 * Run example (two processes):
 *   ./emsgsize_receive_retry_test 0 TCP
 *   ./emsgsize_receive_retry_test 1 TCP
 *
 * Or with mpirun MPMD (even if protocol is TCP or UCX):
 *   mpirun -n 1 ./test_emsgsize_receive 0 TCP : -n 1 ./test_emsgsize_receive 1 TCP
 */

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <unistd.h>

#include "mtcl.hpp"

using namespace MTCL;

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <rank 0|1> [TCP|UCX|...]\n";
        return -1;
    }

    const int rank = std::atoi(argv[1]);

    std::string protocol;
	protocol = argv[2];

	std::string listen_ep = protocol;
	if (protocol=="TCP" || protocol=="UCX") {
		listen_ep += ":127.0.0.1:13000";
	} else if (protocol=="MQTT") {
		listen_ep += ":listen_ep";
	} else {
		MTCL_ERROR("[Test] ", "invalid protocol, expected TCP|UCX|MQTT\n");
		return -1;
	}
	
    if (Manager::init("test_emsgsize") < 0) {
        std::cerr << "Manager::init failed, errno=" << errno << " (" << std::strerror(errno) << ")\n";
        return -1;
    }
	
    // Ensure Node0 is listening even if auto-listen is not enabled in the build
    if (rank == 0) {
        if (Manager::listen(listen_ep) < 0) {
			MTCL_ERROR("[Test] ", "Manager::listen failed on %s, errno=%d (%s)\n", listen_ep.c_str(), errno, std::strerror(errno));
            Manager::finalize(true);
            return -1;
        }
    }

    // Test parameters
    const size_t payload_size = 4096;
    const size_t small_buf_size = 512;

    int rc = 0;

    if (rank == 1) {
        HandleUser h = Manager::connect(listen_ep, 100, 200);
        if (!h.isValid()) {
			MTCL_ERROR("[Test] ", "Client connect failed, errno=%d (%s)\n", errno, std::strerror(errno));
            Manager::finalize(true);
            return -1;
        }

        std::vector<unsigned char> payload(payload_size);
        for (size_t i = 0; i < payload.size(); i++) payload[i] = static_cast<unsigned char>(i % 251);

        ssize_t s = h.send(payload.data(), payload.size());
        if (s != static_cast<ssize_t>(payload.size())) {
			MTCL_ERROR("[Test] ", "Client send failed, ret=%ld, errno=%d (%s)\n", s, errno, std::strerror(errno));
            h.close();
            Manager::finalize(true);
            return -1;
        }

        // Wait for ack so the test is deterministic
        char ack[2];
        errno = 0;
        ssize_t r = h.receive(ack, sizeof(ack));
        if (r != 2 || ack[0] != 'O' || ack[1] != 'K') {
			MTCL_ERROR("[Test] ", "Client ack receive failed, ret=%ld, errno=%d (%s)\n", r, errno, std::strerror(errno));
            rc = 1;
        }

        h.close();
    } else if (rank == 0) {
        // Server
        HandleUser h = Manager::getNext(std::chrono::seconds(100));
        if (!h.isValid()) {
			MTCL_ERROR("[Test] ", "Server getNext timeout, exit\n");
            Manager::finalize(true);
            return -1;
        }

        std::vector<unsigned char> small(small_buf_size);

        errno = 0;
        ssize_t r1 = h.receive(small.data(), small.size());
        if (r1 != -1 || errno != EMSGSIZE) {
			MTCL_ERROR("[Test] ", "Server expected receive() to fail with EMSGSIZE, ret=%ld, errno=%d (%s)\n", r1, errno,
					   std::strerror(errno));
            h.close();
            Manager::finalize(true);
            return -1;
        }

        // Retry with correct size
        std::vector<unsigned char> big(payload_size);

        errno = 0;
        ssize_t r2 = h.receive(big.data(), big.size());
        if (r2 != static_cast<ssize_t>(payload_size)) {
			MTCL_ERROR("[Test] ", "Server retry receive failed, ret=%ld, errno=%d (%s)\n", r2, errno,
					   std::strerror(errno));
            h.close();
            Manager::finalize(true);
            return 1;
        }

        // Validate payload integrity
        for (size_t i = 0; i < big.size(); i++) {
            unsigned char expected = static_cast<unsigned char>(i % 251);
            if (big[i] != expected) {
				MTCL_ERROR("[Test] ", "Server payload mismatch at index %ld, got=%d, expected=%d\n", i,
						   static_cast<int>(big[i]), static_cast<int>(expected));
                rc = -1;
                break;
            }
        }

        // Ack
        const char ack[2] = {'O', 'K'};
        if (h.send(ack, sizeof(ack)) != 2) {
			MTCL_ERROR("[Test] ", "Server ack send failed, errno=%d (%s)\n", errno, strerror(errno));
            rc = -1;
        }

        h.close();
    } else {
		MTCL_ERROR("[Test] ", "Invalid rank, expected 0 or 1\n");
        rc = -1;
    }

    Manager::finalize(true);
	MTCL_ERROR("[Test] ", "Done with %s\n", (rc?"ERROR":"SUCCESS"));
    return rc;
}
