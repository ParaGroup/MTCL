#include <cstdint>
#include <cstring>
#include <iostream>
#include <random>
#include <chrono>
#include <thread>
#include <vector>

#include "mtcl.hpp"

static inline uint64_t now_us() {
    using clock = std::chrono::steady_clock;
    auto t = std::chrono::duration_cast<std::chrono::microseconds>(clock::now().time_since_epoch());
    return (uint64_t)t.count();
}

static constexpr size_t MAX_MESSAGE_SIZE = 2048;

struct MsgHdr {
    uint64_t from;
    uint64_t seq;
    uint64_t kind;   // 0 = PING, 1 = PONG
    uint64_t value;  // seed for deterministic payload
};

static inline void write_hdr(std::vector<uint8_t>& buf, const MsgHdr& h) {
    std::memcpy(buf.data(), &h, sizeof(MsgHdr));
}

static inline MsgHdr read_hdr(const std::vector<uint8_t>& buf) {
    MsgHdr h{};
    std::memcpy(&h, buf.data(), sizeof(MsgHdr));
    return h;
}

static inline void fill_payload(std::vector<uint8_t>& buf, uint64_t seed) {
    for (size_t i = sizeof(MsgHdr); i < buf.size(); ++i) {
        buf[i] = static_cast<uint8_t>((seed + i) & 0xFF);
    }
}

static inline bool check_payload(const std::vector<uint8_t>& buf, uint64_t seed) {
    for (size_t i = sizeof(MsgHdr); i < buf.size(); ++i) {
        if (buf[i] != static_cast<uint8_t>((seed + i) & 0xFF)) return false;
    }
    return true;
}

static void run_pingpong_async(MTCL::HandleUser& h, uint64_t my_id, int iters) {
    std::mt19937_64 rng(my_id ^ 0x1a37f4107f4ffc11ULL);
    std::uniform_int_distribution<uint64_t> dist;

    uint64_t t0 = now_us();
    uint64_t bad = 0;  // how many wrong messages

    for (int i = 0; i < iters; ++i) {
        const size_t min_len = sizeof(MsgHdr);
        const size_t my_len = min_len + (size_t)(dist(rng) % (MAX_MESSAGE_SIZE - min_len + 1));

        MsgHdr my_hdr{};
        my_hdr.from  = my_id;
        my_hdr.seq   = (uint64_t)i;
        my_hdr.kind  = 0;  // PING
        my_hdr.value = dist(rng);

        std::vector<uint8_t> my_ping(my_len);
        write_hdr(my_ping, my_hdr);
        fill_payload(my_ping, my_hdr.value);

        // Post async send and receive. The receive post the maximum size
        MTCL::Request s_ping;
        if (h.isend(my_ping.data(), my_ping.size(), s_ping) < 0) {
            std::perror("isend(PING)");
            return;
        }
        std::vector<uint8_t> peer_ping(MAX_MESSAGE_SIZE);
        MTCL::Request r_ping;
		if (h.ireceive(peer_ping.data(), peer_ping.size(), r_ping) < 0) {
            std::perror("ireceive(PING)");
            return;
        }

        MTCL::waitAll(s_ping, r_ping);
		ssize_t sz = r_ping.count();  // get the actual size received
		MsgHdr peer_hdr{};
		if (sz<=0 || sz < (ssize_t)sizeof(MsgHdr)) {
			++bad;
		} else {
			peer_ping.resize(sz);  // shrink to the actual size
			peer_hdr = read_hdr(peer_ping);
			if (peer_hdr.kind != 0 || !check_payload(peer_ping, peer_hdr.value)) {
				++bad;
			}
		}

        // Build pong: copy peer ping, flip kind, set from
        std::vector<uint8_t> my_pong = peer_ping;
        MsgHdr my_pong_hdr = peer_hdr;
        my_pong_hdr.kind = 1;  // PONG
        my_pong_hdr.from = my_id;
        write_hdr(my_pong, my_pong_hdr);

        MTCL::Request s_pong;
        if (h.isend(my_pong.data(), my_pong.size(), s_pong) < 0) {
            std::perror("isend(PONG)");
            return;
        }

        size_t peer_pong_len = 0;
        if (h.probe(peer_pong_len, true) < 0) {
            std::perror("probe(PONG)");
            return;
        }
        if (peer_pong_len > MAX_MESSAGE_SIZE || peer_pong_len < sizeof(MsgHdr)) {
            std::cerr << "Invalid peer pong size: " << peer_pong_len << std::endl;
            return;
        }

        std::vector<uint8_t> peer_pong(peer_pong_len);
        MTCL::Request r_pong;
        if (h.ireceive(peer_pong.data(), peer_pong.size(), r_pong) < 0) {
            std::perror("ireceive(PONG)");
            return;
        }

        MTCL::waitAll(s_pong, r_pong);

        // Validate pong answers our ping: same seq/value, same total length, correct payload.
        const MsgHdr peer_pong_hdr = read_hdr(peer_pong);
        if (peer_pong_hdr.kind != 1 ||
            peer_pong.size() != my_ping.size() ||
            peer_pong_hdr.seq != my_hdr.seq ||
            peer_pong_hdr.value != my_hdr.value ||
            !check_payload(peer_pong, peer_pong_hdr.value)) {
            ++bad;
        }
    }

    uint64_t t1 = now_us();
    double dt_ms = (double)(t1 - t0) / 1000.0;
    double per_iter_ms = dt_ms / (double)iters;

    std::cout << "Pingpong async done. iters=" << iters
              << " max_size=" << MAX_MESSAGE_SIZE
              << " total_ms=" << dt_ms
              << " per_iter_ms=" << per_iter_ms
              << " bad=" << bad
              << std::endl;
}

int main(int argc, char** argv){
    if(argc < 2) {
        printf("Usage: %s <rank>\n", argv[0]);
        return 1;
    }

    std::string listen_str{};
    std::string connect_str{};

#ifdef ENABLE_TCP
    listen_str = {"TCP:0.0.0.0:42000"};
    connect_str = {"TCP:0.0.0.0:42000"};
#endif

#ifdef ENABLE_MPI
    listen_str = {"MPI:0"};
    connect_str = {"MPI:0"};
#endif

#ifdef ENABLE_MQTT
    listen_str = {"MQTT:0"};
    connect_str = {"MQTT:0:app0"};
#endif

#ifdef ENABLE_UCX
    listen_str = {"UCX:0.0.0.0:42000"};
    connect_str = {"UCX:0.0.0.0:42000"};
#endif

    int rank = atoi(argv[1]);
	MTCL::Manager::init(argv[1]);

    // Listening for new connections, expecting "ping", sending "pong"
    if(rank == 0) {
        MTCL::Manager::listen(listen_str);
		auto handle = MTCL::Manager::getNext();
		run_pingpong_async(handle, rank, 10);
    } else {
		bool connected=false;
		for(int i=0;i<10;++i) {
			auto handle = MTCL::Manager::connect(connect_str);
			if(!handle.isValid()) {
				MTCL_PRINT(0, "Client]:\t", "connection failed\n");
				std::this_thread::sleep_for(std::chrono::seconds(1));
				MTCL_PRINT(0, "Client]:\t", "retry....\n");
			} else {
				connected=true;
				run_pingpong_async(handle, rank, 10);
				break;
			}
		}
		if (!connected) {
			MTCL_PRINT(0, "[Client]:\t", "unable to connect to the server, exit!\n");
			MTCL::Manager::finalize();
			return -1;
		}
    }

    MTCL::Manager::finalize();
    return 0;
}

