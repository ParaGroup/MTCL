#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include <cstring>
#include <atomic>

#include "mtcl.hpp"

using namespace MTCL;

static constexpr int DEFAULT_PORT = 42000;              // for TCP and UCX
static const std::string DEFAULT_LABEL{"listen_label"}; // for MQTT

static uint64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

static void cpu_bound_work_ms(int ms, Request* req, bool do_progress) {
    using clock = std::chrono::steady_clock;
    auto t0 = clock::now();
	std::atomic<uint64_t> x = 0;

    auto last_progress = t0;

    while (std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - t0).count() < ms) {
        // fake CPU-bound work
        for (int i = 0; i < 100000; ++i) x.store( x.load() * 1315423912u + 12345u);

        if (do_progress) {
            auto now = clock::now();
            // Progresso ogni ~1ms
            if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_progress).count() >= 1) {
                test(*req);               // importante per MPI
                //make_progress(*req);    // importante per UCX e (futuro) TCP async
                last_progress = now;
            }
        }
    }
}

int main(int argc, char** argv) {
    size_t size = 4 * 1024 * 1024;
    int work_ms = 5000;
    bool progress = false;

    if (argc < 3 || argc > 6) {
		std::cerr << "use: " << argv[0]
				  << " 0|1 proto [size work_ms 0|1]\n";
		std::cerr << "1st param: 0 sender, 1 receiver\n"
				  << "2nd param: protocol TCP|UCX|MPI|MQTT\n"
				  << "3rd param: optional, message size in byte (default " << size << ")\n"
				  << "4th param: optional, work time in ms (default " << work_ms << ")\n"
				  << "5th param: optional, 0 no progress, 1 enforce progress (MTCL::test) during compute phase (default " << (progress?"1":"0") << ")\n";
		return -1;
	}

	int rank = std::stoi(argv[1]);
	std::string proto{argv[2]};
	if (proto != "MPI" && proto != "TCP" && proto != "UCX" && proto != "MQTT") {
		std::cerr << "Invalid proto; can be MPI|UCX|TCP|MQTT\n";
		return -1;
	}

	if (argc > 3) size = static_cast<size_t>(std::stoull(argv[3]));
	if (argc > 4) work_ms = std::stoi(argv[4]);
	if (argc > 5) progress = (std::stoi(argv[5]) != 0);

	
    Manager::init("");

	std::string ep;
    if (rank == 0) {
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

        auto h = MTCL::Manager::getNext(); // waiting for a readable handle
        if (!h.isValid()) {
            std::cerr << "getNext returned invalid handle\n";
            return -1;
        }

        std::vector<char> buf(size);
        Request req;

        std::cout << "[R] posting ireceive, t=" << now_ms() << " ms\n";
        if (h.ireceive(buf.data(), buf.size(), req) < 0) {
            std::cerr << "ireceive failed, errno=" << errno << " " << strerror(errno) << "\n";
            return -1;
        }

        auto t0 = now_ms();
        std::cout << "[R] waiting recv completion, t0=" << t0 << " ms\n";
        if (req.wait() < 0) {
            std::cerr << "wait failed, errno=" << errno << " " << strerror(errno)
                      << " count=" << req.count() << "\n";
            return -1;
        }
        auto t1 = now_ms();
        std::cout << "[R] recv completed, dt=" << (t1 - t0) << " ms, t=" << t1
                  << " ms, count=" << req.count() << "\n";

    } else { // sender
		if (proto == "TCP" || proto == "UCX") {
			ep = proto + ":localhost:" + std::to_string(DEFAULT_PORT);
		} else if (proto == "MQTT") {
			ep = "MQTT:" + DEFAULT_LABEL;
		} else if (proto == "MPI") {
			ep = "MPI:0";
		}

        auto h = MTCL::Manager::connect(ep);
        if (!h.isValid()) {
            std::cerr << "connect failed, errno=" << errno << " " << strerror(errno) << "\n";
            return -1;
        }

        std::vector<char> buf(size, 'x');
        Request req;

        auto t_post = now_ms();
        std::cout << "[S] posting isend, t=" << t_post << " ms\n";
        if (h.isend(buf.data(), buf.size(), req) < 0) {
            std::cerr << "isend failed, errno=" << errno << " " << strerror(errno) << "\n";
            return -1;
        }

        std::cout << "[S] starting CPU work, work_ms=" << work_ms
                  << " progress=" << (progress ? 1 : 0) << ", t=" << now_ms() << " ms\n";
        cpu_bound_work_ms(work_ms, &req, progress);
        std::cout << "[S] finished CPU work, t=" << now_ms() << " ms\n";

        auto t0 = now_ms();
        std::cout << "[S] waiting send completion, t0=" << t0 << " ms\n";
        if (req.wait() < 0) {
            std::cerr << "wait failed, errno=" << errno << " " << strerror(errno) << "\n";
            return -1;
        }
        auto t1 = now_ms();
        std::cout << "[S] send completed, dt=" << (t1 - t0) << " ms, t1=" << t1 << " ms\n";
    }

    MTCL::Manager::finalize();
    return 0;
}
