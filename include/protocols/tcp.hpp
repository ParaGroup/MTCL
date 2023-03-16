#ifndef TCP_HPP
#define TCP_HPP

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

#include <vector>
#include <queue>
#include <map>
#include <shared_mutex>

#include "../handle.hpp"
#include "../protocolInterface.hpp"



class HandleTCP : public Handle {

    ssize_t readn(int fd, char *ptr, size_t n) {  
        size_t   nleft = n;
        ssize_t  nread;

        while (nleft > 0) {
            if((nread = read(fd, ptr, nleft)) < 0) {
                if (nleft == n) return -1; /* error, return -1 */
                else break; /* error, return amount read so far */
            } else if (nread == 0) break; /* EOF */
            nleft -= nread;
            ptr += nread;
        }
        return(n - nleft); /* return >= 0 */
    }

	ssize_t readvn(int fd, struct iovec *v, int count){
		ssize_t rread;
		for (int cur = 0;;) {
			rread = readv(fd, v+cur, count-cur);
			if (rread <= 0) return rread; // error or closed connection
			while (cur < count && rread >= (ssize_t)v[cur].iov_len)
				rread -= v[cur++].iov_len;
			if (cur == count) return 1; // success!!
			v[cur].iov_base = (char *)v[cur].iov_base + rread;
			v[cur].iov_len -= rread;
		}
		return -1;
	}

    ssize_t writen(int fd, const char *ptr, size_t n) {  
        size_t   nleft = n;
        ssize_t  nwritten;
        
        while (nleft > 0) {
            if((nwritten = write(fd, ptr, nleft)) < 0) {
                if (nleft == n) return -1; /* error, return -1 */
                else break; /* error, return amount written so far */
            } else if (nwritten == 0) break; 
            nleft -= nwritten;
            ptr   += nwritten;
        }
        return(n - nleft); /* return >= 0 */
    }

	ssize_t writevn(int fd, struct iovec *v, int count){
		ssize_t written;
		for (int cur = 0;;) {
			written = writev(fd, v+cur, count-cur);
			if (written < 0) return -1;
			while (cur < count && written >= (ssize_t)v[cur].iov_len)
				written -= v[cur++].iov_len;
			if (cur == count) return 1; // success!!
			v[cur].iov_base = (char *)v[cur].iov_base + written;
			v[cur].iov_len -= written;
		}
		return -1;
	}
	
public:
    int fd; // File descriptor of the connection represented by this Handle
    HandleTCP(ConnType* parent, int fd) : Handle(parent), fd(fd) {}

	ssize_t sendEOS() {
		size_t sz = 0;
		return writen(fd, (char*)&sz, sizeof(size_t)); 
	}
	
    ssize_t send(const void* buff, size_t size) {
		size_t sz = htobe64(size);
        struct iovec iov[2];
        iov[0].iov_base = &sz;
        iov[0].iov_len  = sizeof(sz);
        iov[1].iov_base = const_cast<void*>(buff);
        iov[1].iov_len  = size;

        if (writevn(fd, iov, 2) < 0)
            return -1;
		return size;
    }

	// receives the header containing the size (sizeof(size_t) bytes)
	ssize_t probe(size_t& size, const bool blocking=true) {
		size_t sz;
		ssize_t r;
		if (blocking) {
			if ((r=readn(fd, (char*)&sz, sizeof(size_t)))<=0)
				return r;
		} else {
			if ((r=recv(fd, (char*)&sz, sizeof(size_t), MSG_DONTWAIT))<=0)
				return r;
		}
		size = be64toh(sz);
		return sizeof(size_t);
	}

    bool peek() {
        size_t sz;
        ssize_t r = recv(fd, &sz, sizeof(size_t), MSG_PEEK | MSG_DONTWAIT);
    
        return r;
    }
	
    ssize_t receive(void* buff, size_t size) {
        return readn(fd, (char*)buff, size); 
    }


    ~HandleTCP() {}

};


class ConnTcp : public ConnType {
private:
    // enum class ConnEvent {close, yield};

protected:
    std::string address;
    int port;
    
    std::map<int, Handle*> connections;  // Active connections for this Connector

    fd_set set, tmpset;
    int listen_sck;
#if defined(SINGLE_IO_THREAD)
	int fdmax;
#else	
    std::atomic<int> fdmax;
    std::shared_mutex shm;
#endif

private:

	/**
     * @brief Initializes the main listening socket for this Handle
     * 
     * @return int status code
     */
    int _init() {
        if ((listen_sck=socket(AF_INET, SOCK_STREAM, 0)) < 0){
			MTCL_TCP_PRINT(100, "ConnTcp::_init socket errno=%d\n", errno);
            return -1;
        }

        int enable = 1;
        // enable the reuse of the address
        if (setsockopt(listen_sck, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) {
			MTCL_TCP_PRINT(100, "ConnTcp::_init setsockopt errno=%d\n", errno);
            return -1;
        }

		struct addrinfo hints;
		struct addrinfo *result, *rp;
		memset(&hints, 0, sizeof(hints));
		hints.ai_family   = AF_UNSPEC;    /* Allow IPv4 or IPv6 */
		hints.ai_socktype = SOCK_STREAM;  /* Stream socket */
		hints.ai_flags    = AI_PASSIVE;
		hints.ai_protocol = IPPROTO_TCP;  /* Allow only TCP */
		if (getaddrinfo(address.c_str(), std::to_string(port).c_str(), &hints, &result) != 0) {
			MTCL_TCP_PRINT(100, "ConnTcp::_init getaddrinfo errno=%d\n", errno);		
			return -1;
		}

		bool ok = false;
		for (rp = result; rp != NULL; rp = rp->ai_next) {
			if (bind(listen_sck, rp->ai_addr, (int)rp->ai_addrlen) < 0){
				MTCL_TCP_PRINT(100, "ConnTcp::_init bind errno=%d, continue\n", errno);
				continue;
			}
			ok = true;
			break;
		}
		free(result);
		if (!ok) {
			MTCL_TCP_PRINT(100, "ConnTcp::_init bind loop exit with errno=%d\n", errno);
			return -1;
		}
        if (::listen(listen_sck, TCP_BACKLOG) < 0){
			MTCL_TCP_PRINT(100, "ConnTcp::_init listen errno=%d\n", errno);
            return -1;
        }

        return 0;
    }


public:

   ConnTcp(){};
   ~ConnTcp(){};

    int init(std::string) {
		// For clients who do just connect, the communication thread anyway calls
		// the update method, and we do not want to call the select function with
		// invalid fields.
        FD_ZERO(&set);
        FD_ZERO(&tmpset);
		listen_sck=-1;
		fdmax = -1;
        return 0;
    }

    int listen(std::string s) {
        address = s.substr(0, s.find(":"));
        port = stoi(s.substr(address.length()+1));

		if (this->_init()) {			
			return -1;
		}
		
        MTCL_TCP_PRINT(1, "listen to %s:%d\n", address.c_str(),port);

        // intialize both sets (master, temp)
        FD_ZERO(&set);
        FD_ZERO(&tmpset);

        // add the listen socket to the master set
        FD_SET(this->listen_sck, &set);

        // hold the greater descriptor
        fdmax = this->listen_sck;

        return 0;
    }

    void update() {
        // copy the master set to the temporary

        REMOVE_CODE_IF(std::unique_lock ulock(shm, std::defer_lock));

        REMOVE_CODE_IF(ulock.lock());
        tmpset = set;
        REMOVE_CODE_IF(ulock.unlock());

        struct timeval wait_time = {.tv_sec=0, .tv_usec=TCP_POLL_TIMEOUT};
		int nready=0;
		if (fdmax==-1) return;
        switch(nready=select(fdmax+1, &tmpset, NULL, NULL, &wait_time)) {
		case -1: {
			// NOTE: EBADF can happen because we may close an fd that is in the set
			if (errno==EBADF) {
				MTCL_TCP_PRINT(100, "ConnTcp::update select ERROR: errno=EBADF\n");
				return;
			}			
			MTCL_TCP_ERROR("ConnTcp::update select ERROR: errno=%d -- %s\n", errno, strerror(errno));
		}
		case  0: return;
        }

        for(int idx=0; idx <= fdmax && nready>0; idx++){
            if (FD_ISSET(idx, &tmpset)){
                if (idx == this->listen_sck) {
                    int connfd = accept(this->listen_sck, (struct sockaddr*)NULL ,NULL);
                    if (connfd == -1){
						MTCL_TCP_ERROR("ConnTcp::update accept ERROR: errno=%d -- %s\n", errno, strerror(errno));
                        return;
                    }

                    REMOVE_CODE_IF(ulock.lock());
                    // FD_SET(connfd, &set);
                    // if(connfd > fdmax) {
					// 	fdmax = connfd;
					// }
                    connections[connfd] = new HandleTCP(this, connfd);
					addinQ(true, connections[connfd]);
					REMOVE_CODE_IF(ulock.unlock());                    
                } else {
                    REMOVE_CODE_IF(ulock.lock());

					
                    // Updates ready connections and removes from listening
                    FD_CLR(idx, &set);

                    // update the maximum file descriptor
                    if (idx == fdmax) {
						int ii;
                        for(ii=(fdmax-1);ii>=0;--ii) {
                            if (FD_ISSET(ii, &set)){
                                fdmax = ii;
                                break;
                            }
                        }
						if (ii==-1) fdmax = -1;
					}
					auto it = connections.find(idx);
					if (it != connections.end()) {
						addinQ(false, (*it).second);
					}
                    REMOVE_CODE_IF(ulock.unlock());
                }
				--nready;
            }
        }
    }

    // URL: host:prot || label: stringa utente
    Handle* connect(const std::string& address, int retry, unsigned timeout_ms) {

		int fd=internal_connect(address, retry, timeout_ms);
		if (fd == -1) {
			return nullptr;
		}
		
        HandleTCP *handle = new HandleTCP(this, fd);
		{
			REMOVE_CODE_IF(std::unique_lock lock(shm));
			connections[fd] = handle;
		}
        return handle;
    }

    void notify_close(Handle* h, bool close_wr=true, bool close_rd=true) {
		HandleTCP *handle = reinterpret_cast<HandleTCP*>(h);
		if (close_wr) {
			if (handle->fd != -1) {
				shutdown(handle->fd, SHUT_WR);

				// if the fd is not present in the connections table it means
				// we have already received the EOS and thus executed the
				// notify_close with close_rd=true, we can close the connection
				if (!close_rd &&
					connections.find(handle->fd) == connections.end()) {
					close(handle->fd);
					handle->fd = -1;
				}
			}
		}
		if (close_rd) { 
			HandleTCP *handle = reinterpret_cast<HandleTCP*>(h);
			int fd = handle->fd;
			if (fd==-1) return;
			shutdown(handle->fd, SHUT_RD);
			{
				REMOVE_CODE_IF(std::unique_lock lock(shm));
				connections.erase(fd);
				FD_CLR(fd, &set);
				
				// update the maximum file descriptor
				if (fd == fdmax) {
					int ii;
					for(ii=(fdmax-1);ii>=0;--ii) {
						if (FD_ISSET(ii, &set)){
							fdmax = ii;
							break;
						}
					}
					// the listen socket might not be in the set, thus without the following
					// we risk to leave fdmax set to the old value.
					if (ii==-1) fdmax = -1; 
				}
			}
			if (close_wr) {
				close(fd);
				handle->fd=-1;
			}
		}
    }


    void notify_yield(Handle* h) override {
        int fd = reinterpret_cast<HandleTCP*>(h)->fd;
		if (fd==-1) return;
		REMOVE_CODE_IF(std::unique_lock l(shm));
		if (h->isClosed()) return;
        FD_SET(fd, &set);
        if(fd > fdmax) {
            fdmax = fd;
        }
    }

    void end(bool blockflag=false) {
        auto modified_connections = connections;
        for(auto& [fd, h] : modified_connections) {
			setAsClosed(h, blockflag);
		}
    }

    bool isSet(int fd){
        REMOVE_CODE_IF(std::shared_lock s(shm));
        return FD_ISSET(fd, &set);
    }

};

#endif
