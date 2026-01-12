#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>

#include "mtcl.hpp"

using namespace MTCL;

static constexpr int DEFAULT_PORT = 42000;              // for TCP and UCX
static const std::string DEFAULT_LABEL{"listen_label"}; // for MQTT


static int parse_int_arg(const char* s, int default_value) {
    if (!s) return default_value;
    char* end = nullptr;
    long v = std::strtol(s, &end, 10);
    if (end == s || *end != '\0') return default_value;
    if (v <= 0) return default_value;
    if (v > 1000000000L) return default_value;
    return static_cast<int>(v);
}

static void fail_and_finalize(const char* msg) {
    std::cerr << "[TEST] ERROR: " << msg
              << ", errno=" << errno << " (" << std::strerror(errno) << ")\n";
    Manager::finalize();
    exit(-1);
}

int main(int argc, char** argv) {
	if (argc < 3) {
		std::cerr << "use: " << argv[0] << " 0|1 protocol [#iterations=1000]\n";
		std::cerr << "      possible protocols: MPI, UCX, TCP, MQTT\n";      
		return -1;
	}
	int rank = std::stoi(argv[1]);
	std::string proto{argv[2]};
	if (proto != "MPI" && proto != "TCP" && proto != "UCX" && proto != "MQTT")
		std::cerr << "Invalid proto, can be MPI|UCX|TCP|MQTT\n";
	
    const int num_iterations = (argc > 3) ? parse_int_arg(argv[3], 1000) : 1000;

    if (Manager::init("testAsync") < 0) {
        fail_and_finalize("Manager::init failed");
    }

    // Fixed message sizes used by this test.
    constexpr int kMsg1Len = 35;
    constexpr int kMsg2Len = 10;

    // Message contents.
    // The first message is exactly 35 bytes long.
    const std::string msg1 = "THIS IS THE PAYLOAD OF THE MESSAGE!";
    const std::string msg2(kMsg2Len, 'P');

    if (static_cast<int>(msg1.size()) != kMsg1Len) {
        std::cerr << "[TEST] Internal error: msg1 length is " << msg1.size()
                  << " but expected " << kMsg1Len << ".\n";
        Manager::finalize();
        return -1;
    }

    if (rank == 0) {
        // Rank 0 passively waits for the connection started by rank 1.
		std::string ep{};
		if (proto == "TCP" || proto == "UCX") {
			ep = proto + ":localhost:" + std::to_string(DEFAULT_PORT);
		} else if (proto == "MQTT") {
			ep = "MQTT:" + DEFAULT_LABEL;
		}
		if (ep != "") { // ... for MPI we do not need to listen
			if (Manager::listen(ep) < 0) {
				std::cerr << "Manager::listen failed on " << ep
						  << ", errno=" << errno << " (" << std::strerror(errno) << ")\n";
				Manager::finalize();
				return -1;
			}
		}
        std::cout << "[R0] Waiting for incoming connection...\n";
        auto h = Manager::getNext();
        if (!h.isValid()) {
            fail_and_finalize("Rank 0: getNext returned an invalid handle");
        }

        if (!h.isNewConnection()) {
            std::cerr << "[R0] Expected a new connection handle, got something else.\n";
            h.close();
            Manager::finalize();
            return -1;
        }

        std::cout << "[R0] New connection received, starting async sends (" << num_iterations << " iterations)\n";

        // We post two isend per iteration and wait for both completions.
        // The send buffers must remain valid until waitAll returns.
        RequestPool pool(2);

        for (int i = 0; i < num_iterations; i++) {
            errno = 0;

            if (h.isend(msg1.data(), kMsg1Len, pool)<0)
				fail_and_finalize("Rank 0: isend msg1 error\n");
            if (h.isend(msg2.data(), kMsg2Len, pool)<0)
				fail_and_finalize("Rank 0: isend msg1 error\n");

            pool.waitAll();
            pool.reset();

            if (i == 0 || (i + 1) % 200 == 0) {
                std::cout << "[R0] iter " << (i + 1) << " sent\n";
            }
        }

        h.close();
        std::cout << "[R0] Done, connection closed.\n";
	} else {  // rank 1
		using namespace std::chrono_literals;
		// Rank 1 actively starts the connection to rank 0
		std::string ep{};
		if (proto == "TCP" || proto == "UCX") {
			ep = proto + ":localhost:" + std::to_string(DEFAULT_PORT);
		} else if (proto == "MQTT") {
			ep = "MQTT:" + DEFAULT_LABEL;
		} else if (proto == "MPI") {
			ep = "MPI:0";
		}
		auto h = Manager::connect(ep);
		if (!h.isValid())
			fail_and_finalize("Manager::connect, unable to connect\n");
	
		std::cout << "[R1] Connection established, starting async receives (" << num_iterations << " iterations)\n";

        // We post two ireceive per iteration and wait for both completions.
        // The receive buffers must remain valid until waitAll returns.
        RequestPool rp(2);
		
        for (int i = 0; i < num_iterations; i++) {
            char buf1[kMsg1Len + 1];
            char buf2[kMsg2Len + 1];

            errno = 0;
            if (h.ireceive(buf1, kMsg1Len, rp)<0)
				fail_and_finalize("Rank 1: ireceive buf1 error\n");
            if (h.ireceive(buf2, kMsg2Len, rp)<0)
				fail_and_finalize("Rank 1: ireceive buf2 error\n");

            rp.waitAll();
            rp.reset();
			
            buf1[kMsg1Len] = '\0';
            buf2[kMsg2Len] = '\0';
			
            // Basic sanity checks on received data.
            // We keep them simple so the test output stays readable.
            if (std::string(buf1) != msg1) {
                std::cerr << "[R1] Mismatch on message 1 at iter " << i << "\n"
                          << "     got: \"" << buf1 << "\"\n"
                          << "expected: \"" << msg1 << "\"\n";
                h.close();
                Manager::finalize();
                return 1;
            }
            if (std::string(buf2) != msg2) {
                std::cerr << "[R1] Mismatch on message 2 at iter " << i << "\n";
                h.close();
                Manager::finalize();
                return 1;
            }

            // Print occasionally to avoid flooding stdout.
            if (i == 0 || (i + 1) % 200 == 0) {
                std::cout << "[R1] iter " << (i + 1) << " ok\n";
            }
        }
		
        h.close();
        std::cout << "[R1] Done, connection closed.\n";
    }

    Manager::finalize();
    return 0;
}
