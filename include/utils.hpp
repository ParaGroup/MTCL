#pragma once

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>

#include <sys/types.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <poll.h>
#include <time.h>

#include <cerrno>
#include <cstdarg>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <thread>
#include <chrono>


namespace MTCL {

size_t _registeredProtocols_ = 0;

extern int mtcl_verbose;

#define MTCL_PRINT(LEVEL, prefix, str, ...)								\
	if (MTCL::mtcl_verbose>=LEVEL) MTCL::print_prefix(stdout, str, prefix, ##__VA_ARGS__)
#define MTCL_ERROR(prefix, str, ...)									\
	MTCL::print_prefix(stderr, str, prefix, ##__VA_ARGS__)
#define MTCL_TCP_PRINT(LEVEL, str, ...) MTCL_PRINT(LEVEL, "[MTCL TCP]:",str, ##__VA_ARGS__)
#define MTCL_SHM_PRINT(LEVEL, str, ...) MTCL_PRINT(LEVEL, "[MTCL SHM]:",str, ##__VA_ARGS__)
#define MTCL_UCX_PRINT(LEVEL, str, ...) MTCL_PRINT(LEVEL, "[MTCL UCX]:",str, ##__VA_ARGS__)
#define MTCL_MPI_PRINT(LEVEL, str, ...) MTCL_PRINT(LEVEL, "[MTCL MPI]:",str, ##__VA_ARGS__)
#define MTCL_MQTT_PRINT(LEVEL, str, ...) MTCL_PRINT(LEVEL, "[MTCL MQTT]:",str, ##__VA_ARGS__)
#define MTCL_MPIP2P_PRINT(LEVEL, str, ...) MTCL_PRINT(LEVEL, "[MTCL MPIP2P]:",str, ##__VA_ARGS__)
#define MTCL_TCP_ERROR(str, ...) MTCL_ERROR("[MTCL TCP]:",str, ##__VA_ARGS__)
#define MTCL_SHM_ERROR(str, ...) MTCL_ERROR("[MTCL SHM]:",str, ##__VA_ARGS__)
#define MTCL_UCX_ERROR(str, ...) MTCL_ERROR("[MTCL UCX]:",str, ##__VA_ARGS__)
#define MTCL_MPI_ERROR(str, ...) MTCL_ERROR("[MTCL MPI]:",str, ##__VA_ARGS__)
#define MTCL_MQTT_ERROR(str, ...) MTCL_ERROR("[MTCL MQTT]:",str, ##__VA_ARGS__)
#define MTCL_MPIP2P_ERROR(str, ...) MTCL_ERROR("[MTCL MPIP2P]:",str, ##__VA_ARGS__)

static inline void print_prefix(FILE *stream, const char *str, const char *prefix, ...) {
    va_list argp;
    va_start(argp, prefix);
    fprintf(stream, "%-13s", prefix);
    vfprintf(stream, str, argp);
    fflush(stream);
    va_end(argp);
}

std::string getPoolFromHost(const std::string& host){
    auto pos = host.find(':');
    if (pos == std::string::npos) return {};
    return host.substr(0, pos);
}

static inline bool splitProtoRest(const std::string& s, std::string& proto, std::string& rest) {
	auto pos = s.find(':');
	if (pos == std::string::npos) return false;
	proto = s.substr(0, pos);
	rest  = s.substr(pos + 1);
	return true;
}


	
static inline void mtcl_cpu_relax(void) {
#if defined(__x86_64__) || defined(__i386__)
#define MTCL_PAUSE() __asm__ __volatile__("pause")
#elif defined(__aarch64__) || defined(__arm__)
  #define MTCL_PAUSE() __asm__ __volatile__("yield")
#elif defined(__powerpc__) || defined(__ppc__) || defined(__PPC__)
  #define MTCL_PAUSE() __asm__ __volatile__("or 27,27,27")
#elif defined(__riscv)
// compile with -DMTCL_RISCV_HAS_PAUSE=1 to switch to "pause" if supported
  #if defined(MTCL_RISCV_HAS_PAUSE)
    #define MTCL_PAUSE() __asm__ __volatile__("pause")
  #else
    #define MTCL_PAUSE() __asm__ __volatile__("nop")
  #endif
#else
// portable fallback
  #define MTCL_PAUSE() __asm__ __volatile__("")
#endif

	MTCL_PAUSE();
}

	
/**
 * @brief Non-blocking probe loop with a bounded timeout.
 *
 * Repeatedly calls 'h->probe(size, false)' until the first fragment is
 * available or a deadline expires. It never blocks indefinitely on half-open 
 * or stalled connections.
 *
 * Parameters:
 * - retry: number of retry windows. If retry <= 0 it is treated as 1.
 * - timeout_ms: milliseconds per retry window. Total deadline is:
 *     total_timeout = timeout_ms * max(retry, 1)
 *   If timeout_ms is 0, the function performs a single non-blocking check and
 *   then times out immediately.
 *
 * Return and errno mapping:
 * - Returns > 0 when 'probe()' reports a header is available, and sets 'size'.
 * - Returns -1 on failure:
 *   - If 'probe()' returns 0 (EOS), sets errno = ECONNRESET.
 *   - If the deadline expires, sets errno = ETIMEDOUT.
 *   - If 'probe()' fails with an error different from EWOULDBLOCK/EAGAIN, it is propagated.
 *
 * Polling strategy:
 * - First it spins aggressively using mtcl_cpu_relax() for low latency.
 * - If data does not arrive quickly, it backs off with short sleeps to avoid burning a core.
 */
template<typename H>
static inline ssize_t nb_probe_with_timeout(H* h, size_t& size,	int retry, unsigned timeout_ms) {
	static_assert(std::is_pointer_v<decltype(h)>, "h must be a pointer");
    // This check ensures H has a method probe(size_t&, bool) returning ssize_t
    static_assert(std::is_same_v<decltype(h->probe(size, false)), ssize_t>,
                  "H must implement: ssize_t probe(size_t& size, bool blocking)");
    if (retry <= 0) retry = 1;

    using clock = std::chrono::steady_clock;

    const auto start = clock::now();
    const auto deadline = start + std::chrono::milliseconds((uint64_t)timeout_ms * (uint64_t)retry);
    constexpr auto SPIN_BUDGET = std::chrono::microseconds(SPIN_THRESHOLD);
    constexpr auto MAX_SLEEP = std::chrono::microseconds(WAIT_INTERNAL_TIMEOUT);

    auto sleep = std::chrono::microseconds(1);

    while (true) {
        const ssize_t r = h->probe(size, false);

        if (r > 0) return r;
        if (r == 0) { errno = ECONNRESET; return -1; }

        if (errno != EWOULDBLOCK && errno != EAGAIN) return -1;

        const auto now = clock::now();
        if (now >= deadline) { errno = ETIMEDOUT; return -1; }

        const auto elapsed = now - start;

        if (elapsed < SPIN_BUDGET) { // fast path
            mtcl_cpu_relax();
        } else {
            // Backoff to reduce CPU usage on slow peers or half-open connections
            std::this_thread::sleep_for(sleep);
            sleep = std::min(sleep * 2, MAX_SLEEP);
        }
    }
}

	
// -------------------- TCP utilty functions -----------------------------------

static inline int internal_connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
	int saved_errno=0;
    int rc = 0;
    // Set O_NONBLOCK
    int sockfd_flags_before;
    if((sockfd_flags_before=fcntl(sockfd,F_GETFL,0)<0)) return -1;
    if(fcntl(sockfd,F_SETFL,sockfd_flags_before | O_NONBLOCK)<0) return -1;
    // Start connecting (asynchronously)
    do {
        if (connect(sockfd, addr, addrlen)<0) {
            // Did connect return an error? If so, we'll fail.
            if ((errno != EWOULDBLOCK) && (errno != EINPROGRESS)) {
				rc = -1;
				saved_errno=errno;
            } else {  // Otherwise, we'll wait for it to complete.
				saved_errno= EHOSTUNREACH;

                // Set a deadline timestamp 'timeout' ms from now (needed b/c poll can be interrupted)
                struct timespec now;
                if(clock_gettime(CLOCK_MONOTONIC, &now)<0) { rc=-1; break; }
                struct timespec deadline = { .tv_sec = now.tv_sec,
                                             .tv_nsec = now.tv_nsec + (UNREACHABLE_ADDR_TIMOUT*1000000l)};
                // Wait for the connection to complete.
                do {
                    // Calculate how long until the deadline
                    if(clock_gettime(CLOCK_MONOTONIC, &now)<0) {rc=-1; break;}

                    int ms_until_deadline = (int)(  (deadline.tv_sec  - now.tv_sec)*1000l
													+ (deadline.tv_nsec - now.tv_nsec)/1000000l);
                    if(ms_until_deadline<0) { rc=0;	break; }
                    // Wait for connect to complete (or for the timeout deadline)
                    struct pollfd pfds[] = { { .fd = sockfd, .events = POLLOUT } };
					
                    rc = poll(pfds, 1, ms_until_deadline);
                    // If poll 'succeeded', make sure it *really* succeeded
                    if(rc>0) {
                        int error = 0; socklen_t len = sizeof(error);
                        int retval = getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &error, &len);
                        if(retval==0) saved_errno = error;
                        if(error!=0)  rc=-1; 
                    }
                } while(rc==-1 && errno==EINTR); // If poll was interrupted, try again.
                if(rc==0) rc=-1;                 // Did poll timeout? If so, fail.
            }
        }
    } while(0);
    // Restore original O_NONBLOCK state
    if(fcntl(sockfd,F_SETFL,sockfd_flags_before)<0) return -1;
	errno = saved_errno;
    return rc;
}


static inline int internal_connect(const std::string& address, int retry, unsigned timeout_ms) {
	const std::string host = address.substr(0, address.find(":"));
	const std::string svc  = address.substr(host.length()+1);
	
	MTCL_PRINT(100, "[MTCL]:", "connecting to %s:%s\n", host.c_str(), svc.c_str());
	
	int fd=-1;	
	struct addrinfo hints;
	struct addrinfo *result, *rp;
	
	memset(&hints, 0, sizeof(hints));
	hints.ai_family   = AF_UNSPEC;              /* Allow IPv4 or IPv6 */
	hints.ai_socktype = SOCK_STREAM;            /* Stream socket */
	hints.ai_flags    = 0;
	hints.ai_protocol = IPPROTO_TCP;            /* Allow only TCP */
	
	// resolve the address (assumo stringa formattata come host:port)
	if (getaddrinfo(host.c_str(), svc.c_str(), &hints, &result) != 0) {
		MTCL_PRINT(100, "MTCL:", "internal_connect  getaddrinfo error, errno=%d\n", errno);
		return -1;
	}

	bool connected=false;
	do {			
		// try to connect to a possible one of the resolution results
		for (rp = result; rp != NULL; rp = rp->ai_next) {
			fd = socket(rp->ai_family, rp->ai_socktype,
						rp->ai_protocol);
			if (fd == -1) {
				MTCL_PRINT(100, "[MTCL]:", "internal_connect socket error, errno=%d\n", errno);
				continue;
			}
						
			if (internal_connect(fd, rp->ai_addr, rp->ai_addrlen) != -1) {			   
				retry=0;     
				connected=true;
				break;                  /* Success */
			} 
			close(fd);
		}
		if (!connected && retry>0) {
			--retry;		
			std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
			MTCL_PRINT(100, "[MTCL]:", "retry to connect to %s:%s\n", host.c_str(), svc.c_str());
		}
	} while(retry>0);
	
	free(result);
    
	if (rp == NULL) /* No address succeeded */
		return -1;
	return fd;
}


} // namespace


// if NO_MTCL_MULTITHREADED is defined, we do not use locking for accessing
// internal data structures, thus some code can be removed.
// A different case is for the MPIP2P transport protocol.
#if defined(NO_MTCL_MULTITHREADED)
#define SINGLE_IO_THREAD
#define REMOVE_CODE_IF(X)
#define ADD_CODE_IF(X)    X
#else
#define REMOVE_CODE_IF(X) X
#define ADD_CODE_IF(X) 
#endif


#ifdef __APPLE__
    #include <libkern/OSByteOrder.h>
    #define htobe16(x) OSSwapHostToBigInt16(x)
    #define htole16(x) OSSwapHostToLittleInt16(x)
    #define be16toh(x) OSSwapBigToHostInt16(x)
    #define le16toh(x) OSSwapLittleToHostInt16(x)

    #define htobe32(x) OSSwapHostToBigInt32(x)
    #define htole32(x) OSSwapHostToLittleInt32(x)
    #define be32toh(x) OSSwapBigToHostInt32(x)
    #define le32toh(x) OSSwapLittleToHostInt32(x)

    #define htobe64(x) OSSwapHostToBigInt64(x)
    #define htole64(x) OSSwapHostToLittleInt64(x)
    #define be64toh(x) OSSwapBigToHostInt64(x)
    #define le64toh(x) OSSwapLittleToHostInt64(x)

    #ifndef UIO_MAXIOV
    #define UIO_MAXIOV 1023
    #endif

    #define ECOMM 1147
#endif // __APPLE__

