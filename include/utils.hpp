#ifndef UTILS_HPP
#define UTILS_HPP

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

extern int mtcl_verbose;

#define MTCL_PRINT(LEVEL, prefix, str, ...)								\
	if (mtcl_verbose>=LEVEL) print_prefix(stdout, str, prefix, ##__VA_ARGS__)
#define MTCL_ERROR(prefix, str, ...)									\
	print_prefix(stderr, str, prefix, ##__VA_ARGS__)
#define MTCL_TCP_PRINT(LEVEL, str, ...) MTCL_PRINT(LEVEL, "[MTCL TCP]:\t",str, ##__VA_ARGS__)
#define MTCL_SHM_PRINT(LEVEL, str, ...) MTCL_PRINT(LEVEL, "[MTCL SHM]:\t",str, ##__VA_ARGS__)
#define MTCL_UCX_PRINT(LEVEL, str, ...) MTCL_PRINT(LEVEL, "[MTCL UCX]:\t",str, ##__VA_ARGS__)
#define MTCL_MPI_PRINT(LEVEL, str, ...) MTCL_PRINT(LEVEL, "[MTCL MPI]:\t",str, ##__VA_ARGS__)
#define MTCL_MQTT_PRINT(LEVEL, str, ...) MTCL_PRINT(LEVEL, "[MTCL MQTT]:\t",str, ##__VA_ARGS__)
#define MTCL_MPIP2P_PRINT(LEVEL, str, ...) MTCL_PRINT(LEVEL, "[MTCL MPIP2P]:\t",str, ##__VA_ARGS__)
#define MTCL_TCP_ERROR(str, ...) MTCL_ERROR("[MTCL TCP]:\t",str, ##__VA_ARGS__)
#define MTCL_SHM_ERROR(str, ...) MTCL_ERROR("[MTCL SHM]:\t",str, ##__VA_ARGS__)
#define MTCL_UCX_ERROR(str, ...) MTCL_ERROR("[MTCL UCX]:\t",str, ##__VA_ARGS__)
#define MTCL_MPI_ERROR(str, ...) MTCL_ERROR("[MTCL MPI]:\t",str, ##__VA_ARGS__)
#define MTCL_MQTT_ERROR(str, ...) MTCL_ERROR("[MTCL MQTT]:\t",str, ##__VA_ARGS__)
#define MTCL_MPIP2P_ERROR(str, ...) MTCL_ERROR("[MTCL MPIP2P]:\t",str, ##__VA_ARGS__)

static inline void print_prefix(FILE *stream, const char * str, const char *prefix, ...) {
    va_list argp;
    char * p=(char *)malloc(strlen(str)+strlen(prefix)+1);
    if (!p) {
		perror("malloc");
        fprintf(stderr,"FATAL ERROR in print_prefix\n");
        return;
    }
    strcpy(p,prefix);
    strcpy(p+strlen(prefix), str);
    va_start(argp, prefix);
    vfprintf(stream, p, argp);
	fflush(stream);
    va_end(argp);
    free(p);
}


// if SINGLE_IO_THREAD is defined, we do not use locking for accessing
// internal data structures, thus some code can be removed.
// A different case is for the MPIP2P transport protocol.
#if defined(SINGLE_IO_THREAD)
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


std::string getPoolFromHost(const std::string& host){
    auto pos = host.find(':');
    if (pos == std::string::npos) return {};
    return host.substr(0, pos);
}

#if defined(__i386__) || defined(__x86_64__)
#define PAUSE()  __asm__ __volatile__ ("rep; nop" ::: "memory")
#endif // __i386

#if defined (__riscv)
#define PAUSE()  /* ?? */
#endif  // __riscv

#if defined(__powerpc__) || defined(__ppc__)
// yield   ==   or 27, 27, 27
#define PAUSE()  asm volatile ("or 27,27,27" ::: "memory");
#endif // __powerpc

#if defined(__arm__) || defined(__aarch64__)
#define PAUSE()  asm volatile("yield" ::: "memory")
#endif //__arm

static __always_inline void cpu_relax(void) {
	PAUSE();
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
	
	MTCL_PRINT(100, "[MTCL]", "connecting to %s:%s\n", host.c_str(), svc.c_str());
	
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
		MTCL_PRINT(100, "MTCL", "internal_connect  getaddrinfo error, errno=%d\n", errno);
		return -1;
	}

	bool connected=false;
	do {			
		// try to connect to a possible one of the resolution results
		for (rp = result; rp != NULL; rp = rp->ai_next) {
			fd = socket(rp->ai_family, rp->ai_socktype,
						rp->ai_protocol);
			if (fd == -1) {
				MTCL_PRINT(100, "[MTCL]", "internal_connect socket error, errno=%d\n", errno);
				continue;
			}
						
			if (internal_connect(fd, rp->ai_addr, rp->ai_addrlen) != -1) {			   
				retry=0;     
				connected=true;
				break;                  /* Success */
			} 
			close(fd);
		}
		if (!connected) {
			--retry;		
			std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
			MTCL_PRINT(100, "MTCL", "retry to connect to %s:%s\n", host.c_str(), svc.c_str());
		}
	} while(retry>0);
	
	free(result);
    
	if (rp == NULL) /* No address succeeded */
		return -1;
	return fd;
}




#endif
