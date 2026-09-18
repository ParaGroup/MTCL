#pragma once

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>

#include <sys/types.h>
#include <sys/uio.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <poll.h>
#include <time.h>

#include <cstdint>
#include <deque>
#include <mutex>
#include <vector>
#include <queue>
#include <map>
#include <unordered_map>
#include <shared_mutex>
#include <atomic>
#include <stdexcept>
#include <thread>
#include <chrono>
#include <algorithm>

#include "../handle.hpp"
#include "../protocolInterface.hpp"
#include "../utils.hpp"

namespace MTCL {

static inline int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static inline int set_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
}

class HandleTCP;

class RequestTCP : public request_internal {
    friend class TcpRequestVector;

// private get functions for the arena
private:
    static char*& get_arena() {
        static char* arena = nullptr;
        return arena;
    }
    static size_t& get_arena_capacity() {
        static size_t cap = 0;
        return cap;
    }
    static std::vector<void*>& get_free_list() {
        static std::vector<void*> fl;
        return fl;
    }
    static std::mutex& get_pool_mutex() {
        static std::mutex m;
        return m;
    }

public:
    enum class State : int { IDLE, BUSY, COMPLETED, FAILED };
    enum class OpType : int { SEND, RECEIVE }; // What type of request it's handling

    // one private arena for each requestPool
    struct PrivateArena {
        char* data;
        size_t capacity; // number of requests defined during the creation of the requestPool
        size_t used;

        PrivateArena(size_t count)
            : capacity(count), used(0)
        {
            data = static_cast<char*>(::operator new(count * sizeof(RequestTCP)));
        }

        ~PrivateArena() {
            ::operator delete(data);
        }

        PrivateArena(const PrivateArena&) = delete;
        PrivateArena& operator=(const PrivateArena&) = delete;
        PrivateArena(PrivateArena&& o) noexcept
            : data(o.data), capacity(o.capacity), used(o.used)
        {
            o.data = nullptr;
            o.capacity = 0;
            o.used = 0;
        }

        RequestTCP* allocate() {
            if (used >= capacity) return nullptr;
            void* slot = data + (used * sizeof(RequestTCP));
            used++;
            return new (slot) RequestTCP();
        }

        void reset() {
            for (size_t i = 0; i < used; ++i) {
                auto* req = reinterpret_cast<RequestTCP*>(
                    data + (i * sizeof(RequestTCP)));
                req->~RequestTCP();
            }
            used = 0;
        }
    };

    // Instantiate the arena with a total of 'max_requests' requests.
    static void init_pool(size_t max_requests = TCP_REQUEST_POOL_SIZE) {
        std::lock_guard<std::mutex> lk(get_pool_mutex());
        if (get_arena() != nullptr) return;

        get_arena_capacity() = max_requests;
        get_arena() = static_cast<char*>(
            ::operator new(max_requests * sizeof(RequestTCP)));

        auto& fl = get_free_list();
        fl.reserve(max_requests);
        for (size_t i = 0; i < max_requests; ++i) {
            fl.push_back(get_arena() + (i * sizeof(RequestTCP)));
        }
    }

    static void destroy_pool() {
        std::lock_guard<std::mutex> lk(get_pool_mutex());
        if (get_arena()) {
            ::operator delete(get_arena());
            get_arena() = nullptr;
            get_arena_capacity() = 0;
            get_free_list().clear();
        }
    }

    // The new operator returns one of the Requests allocated if available
    static void* operator new(size_t) noexcept {
        std::lock_guard<std::mutex> lk(get_pool_mutex());
        auto& fl = get_free_list();
        if (fl.empty()) {
            errno = ENOMEM;
            return nullptr;
        }
        void* ptr = fl.back();
        fl.pop_back();
        return ptr;
    }

    // The delete operator pushes back the request in the free list
    static void operator delete(void* ptr) {
        if (!ptr) return;
        std::lock_guard<std::mutex> lk(get_pool_mutex());
        get_free_list().push_back(ptr);
    }

    static void* operator new(size_t, void* ptr) {
        return ptr;
    }

    static void operator delete(void*, void*) {}

private:
    int fd;
    /**
     * user_buffer and user_buffer_size are used in both send and receive
     * in send they are the payload to be sent and the size of the payload
     * in receive they are the buffer to be filled and it's size
     */
    char* user_buffer;
    size_t user_buffer_size;

    std::pair<bool, size_t>* probed;

    size_t payload_total;
    std::atomic<size_t> payload_read;
    std::atomic<size_t> payload_written;
    uint64_t szbe;

    int error_code;
    bool blocking;
    OpType operation;

    struct iovec iov[2];
    static constexpr size_t HDR_SZ = sizeof(uint64_t);

    std::atomic<State> request_state{State::IDLE};
    HandleTCP* parent_handle;

    int readn(char* ptr, size_t target_size, size_t& current_offset) {
        size_t nleft = target_size - current_offset;

        while (nleft > 0) {
            ssize_t nread = ::read(fd, ptr + current_offset, nleft);
            if (nread < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    if (!blocking) return 0;
                    continue;
                }
                if (errno == EINTR) continue;
                error_code = errno;
                return -1;

            } else if (nread == 0) {
                error_code = ECONNRESET;
                errno = ECONNRESET;
                return -1;
            }

            current_offset += nread;
            nleft -= nread;

            // Continue reading data until it gets EWOULDBLOCK or EAGAIN
        }
        return 1;
    }

    ssize_t writevn(struct iovec *v, int count) {
        int cur = 0;
        while (cur < count) {
            // ignore iovec already empty
            while (cur < count && v[cur].iov_len == 0) {
                cur++;
            }
            if (cur == count) return 1;

            ssize_t local_write = writev(fd, v + cur, count - cur);

            if (local_write < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    if(!blocking) return 0;
                    continue;
                }
                error_code = errno;
                return -1;
            }

            // Connection closed by the remote host
            if (local_write == 0) {
                error_code = ECONNRESET;
                errno = ECONNRESET;
                return -1;
            }

            size_t remaining = local_write;
            while (cur < count && remaining > 0) {
                if (remaining >= v[cur].iov_len) {
                    remaining -= v[cur].iov_len;
                    v[cur].iov_len = 0;
                    cur++;
                } else {
                    v[cur].iov_base = static_cast<char*>(v[cur].iov_base) + remaining;
                    v[cur].iov_len -= remaining;
                    remaining = 0;
                }
            }
        }
        return 1;
    }

public:
    RequestTCP() : fd(-1), user_buffer(nullptr), user_buffer_size(0),
                          probed(nullptr), payload_total(0), payload_read(0), payload_written(0),
                          error_code(0), blocking(false), parent_handle(nullptr) {}

    void reinit(int _fd, void* _buff, size_t _size,
                std::pair<bool, size_t>* _probed,
                HandleTCP* _parent_handle,
                bool _operation, // operation is true for send, false for receive
                bool _blocking = false)
    {
        fd = _fd;
        user_buffer = static_cast<char*>(_buff);
        user_buffer_size = _size;
        probed = _probed;
        if(_operation){ // send
            operation = OpType::SEND;
            szbe = htobe64((uint64_t)user_buffer_size);
            iov[0].iov_base = &szbe;
            iov[0].iov_len = HDR_SZ;
            iov[1].iov_base = user_buffer;
            iov[1].iov_len = user_buffer_size;
        } else
            operation = OpType::RECEIVE;

        blocking = _blocking;

        error_code = 0;
        payload_read.store(0, std::memory_order_relaxed);
        payload_total = 0;
        payload_written.store(0, std::memory_order_relaxed);
        parent_handle = _parent_handle;
        request_state.store(State::IDLE, std::memory_order_relaxed);
    }

    void invalidate_handle();

    /**
     * @brief Attempts to advance the I/O state of this request.
     * * THREAD SAFETY: This method can be called concurrently by the IO multiplexer thread
     * (via ConnTcp::update) and the user thread (via wait()).
     * The compare_exchange_strong guarantees that only one thread "owns" the right to
     * perform the read/write syscall at any given time, preventing interleaved data corruption.
     * * @return 1 if completed, -1 if failed, 0 if it yielded (EAGAIN/EWOULDBLOCK).
     */
    int doWork() {
        State expected = State::IDLE;
        if (!request_state.compare_exchange_strong(expected, State::BUSY,
                                                   std::memory_order_acquire)) {
            if (expected == State::COMPLETED) return 1;
            if (expected == State::FAILED) return -1;
            return 0;
        }

        if (error_code != 0) {
            request_state.store(State::FAILED, std::memory_order_release);
            return -1;
        }

        if (operation == OpType::RECEIVE)
            return receiveMsg();
        else
            return sendMsg();
    }

    // receiveMsg, sendMsg, test and wait are defined at the end of the file
    int receiveMsg();
    int sendMsg();

    int test(int& result) override;

    int wait() override;

    int check_status() {
        State current_state = request_state.load(std::memory_order_acquire);
        if (current_state == State::COMPLETED) return 1;
        if (current_state == State::FAILED) return -1;
        return 0;
    }

    void removeFromDeque();

    ssize_t count() const override {
        if(operation == OpType::RECEIVE)
            return payload_read.load(std::memory_order_relaxed);
        else
            return payload_written.load(std::memory_order_relaxed);
    }

    void release();
    ~RequestTCP() override {
        if (check_status() == 0) {
            invalidate_handle();
        } else {
            removeFromDeque();
        }
    }
};

// organize requestPool arenas
class TcpRequestVector : public ConnRequestVector {
private:
    std::vector<RequestTCP*> send_requests;
    std::vector<RequestTCP*> recv_requests;
    RequestTCP::PrivateArena arena;
    std::mutex pool_mutex;

public:
    explicit TcpRequestVector(size_t count)
        : arena(count) // Instantiate 'count' requests in the Pool
    {
        send_requests.reserve(count);
        recv_requests.reserve(count);
    }

    TcpRequestVector() : arena(0) {}

    // Get one request from requestPool
    RequestTCP* allocate(bool is_send) {
        std::lock_guard<std::mutex> lk(pool_mutex);
        RequestTCP* req = arena.allocate();
        if (!req) return nullptr;
        if(is_send)
            send_requests.push_back(req);
        else
            recv_requests.push_back(req);
        return req;
    }

    void waitAll() override {
        std::vector<RequestTCP*> snd_copy, rcv_copy;
        {
            std::lock_guard<std::mutex> lk(pool_mutex);
            snd_copy = send_requests;
            rcv_copy = recv_requests;
        }

        for (auto* req : snd_copy)
            req->wait();
        for(auto* req : rcv_copy)
            req->wait();
    }

    bool testAll() override {
        std::lock_guard<std::mutex> lk(pool_mutex);

        bool all_completed = true;
        for (auto* req : send_requests) {
            int is_done = 0;
            if (req->test(is_done) < 0) continue;
            if (is_done == 0) all_completed = false;
        }
        for (auto* req : recv_requests) {
            int is_done = 0;
            if (req->test(is_done) < 0) continue;
            if (is_done == 0) all_completed = false;
        }
        return all_completed;
    }

    void reset() override {
        std::lock_guard<std::mutex> lk(pool_mutex);
        send_requests.clear();
        recv_requests.clear();
        arena.reset();
    }

    ~TcpRequestVector() {
        auto cleanup = [](std::vector<RequestTCP*>& vec) {
            for (auto* req : vec) {
                if (req->check_status() == 0) {
                    req->invalidate_handle();
                }
                req->removeFromDeque();
            }
        };

        cleanup(send_requests);
        cleanup(recv_requests);
        reset();
     }
};

class HandleTCP : public Handle {
    friend class RequestTCP;

    std::deque<RequestTCP*> active_req_receive;
    std::mutex rcv_mutex;

    std::deque<RequestTCP*> active_req_send;
    std::mutex send_mutex;

    uint64_t header_buf = 0;
    size_t header_read = 0;

    std::atomic<size_t> drain_payload_remaining{0}; // How many bytes left to read
    std::atomic<bool> is_draining_header{false};    // Header not fully read

    bool needs_drain() {
        return is_draining_header.load(std::memory_order_acquire) ||
            drain_payload_remaining.load(std::memory_order_acquire) > 0;
    }

    ssize_t readvn(int fd, struct iovec *v, int count) {
        ssize_t rread;
        for (int cur = 0;;) {
            rread = readv(fd, v + cur, count - cur);
            if (rread < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    std::this_thread::yield();
                    continue;
                }
                return -1;
            } else if (rread == 0) return 0;
            while (cur < count && rread >= (ssize_t)v[cur].iov_len)
                rread -= v[cur++].iov_len;
            if (cur == count) return 1; // success!!
            v[cur].iov_base = (char *)v[cur].iov_base + rread;
            v[cur].iov_len -= rread;
        }
        return -1;
    }

    ssize_t writen(int fd, const char *ptr, size_t n) {
        size_t nleft = n;
        ssize_t nwritten;

        while (nleft > 0) {
            if ((nwritten = write(fd, ptr, nleft)) < 0) {
                if (nleft == n) return -1; /* error, return -1 */
                else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    MTCL::mtcl_cpu_relax();
                    continue;
                }
                else break; /* error, return amount written so far */
            } else if (nwritten == 0) break;
            nleft -= nwritten;
            ptr += nwritten;
        }
        return (n - nleft); /* return >= 0 */
    }

public:
    int fd; // File descriptor of the connection represented by this Handle
    std::atomic<bool> is_yielded{false};

    HandleTCP(ConnType* parent, int fd) : Handle(parent), fd(fd) {
        RequestTCP::init_pool();
    }

    static constexpr size_t HDR_SZ = sizeof(uint64_t);

    bool has_active_receive_req() {
        std::lock_guard<std::mutex> lk(rcv_mutex);
        return !active_req_receive.empty();
    }

    bool has_active_send_req() {
        std::lock_guard<std::mutex> lk(send_mutex);
        return !active_req_send.empty();
    }

    ssize_t sendEOS() {
        const uint64_t szbe = htobe64((uint64_t)0);
        return writen(fd, (char*)&szbe, HDR_SZ);
    }

    ssize_t send(const void* buff, size_t size) {

        RequestTCP* req = new RequestTCP();
        if(!req)
            return -1;

        // We could get a recycled request, so we re-initialize it
        req->reinit(fd, const_cast<void*>(buff), size, nullptr, this, true, true);

        {
            std::lock_guard<std::mutex>lk(send_mutex);
            active_req_send.push_back(req);
        }

        // Since it's a synchronous request, it needs to complete all the previous ones.
        int ret = req->wait();
        ssize_t sent_bytes = req->count();

        delete req; // The request goes back in the pool of available requests

        if(ret < 0)
            return -1;
        if (ret == 0)
            return (sent_bytes == (ssize_t)size) ? sent_bytes : -1;
        return -1;
    }

    ssize_t isend(const void* buff, size_t size, Request& r) {

        RequestTCP* req = new RequestTCP();
        if(!req)
            return -1;

        req->reinit(fd, const_cast<void*>(buff), size, nullptr, this, true);
        r.__setInternalR(req);

        {
            std::lock_guard<std::mutex>lk(send_mutex);
            active_req_send.push_back(req);
        }

        // Make some progress
        if(continue_send_req()<0){
            return -1;
        }
        return 0;
    }

    ssize_t isend(const void* buff, size_t size, RequestPool& r) {

        TcpRequestVector* tcp_vec = r._getInternalVector<TcpRequestVector>();
        RequestTCP* req = tcp_vec->allocate(true);
        if(!req) {
            errno = ENOMEM;
            return -1;
        }

        req->reinit(fd, const_cast<void*>(buff), size, nullptr, this, true);
        {
            std::lock_guard<std::mutex>lk(send_mutex);
            active_req_send.push_back(req);
        }

        // Make some progress
        continue_send_req();

        return 0;
    }

    // receives the header containing the size (HDR_SZ bytes)
    ssize_t probe(size_t& size, const bool blocking = true) {
        if (probed.first) {
            size = probed.second;
            return (ssize_t)HDR_SZ;
        }

        if (!blocking && header_read == 0) {
            const ssize_t pr = ::recv(fd, (char*)&header_buf, HDR_SZ,
                                      MSG_PEEK | MSG_DONTWAIT);
            if (pr < 0) {
                if (errno != EAGAIN && errno != EWOULDBLOCK)
                    return -1;       // errno set by recv
            }
            if (pr == 0) {
                errno = ECONNRESET;
                return -1;
            }
            if (pr < (ssize_t)HDR_SZ) { // header not fully available yet
                errno = EWOULDBLOCK;
                return -1;
            }
        }

        while (header_read < HDR_SZ) {
            ssize_t nread = ::read(fd, (char*)&header_buf + header_read,
                                   HDR_SZ - header_read);

            if (nread < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    if (!blocking) return -1;
                    MTCL::mtcl_cpu_relax();
                    continue;
                }
                if (errno == EINTR) continue;
                return -1;
            } else if (nread == 0) {
                errno = ECONNRESET;
                return -1;
            }
            header_read += nread;
        }

        size = (size_t)be64toh(header_buf);
        probed = {true, size};

        header_read = 0;
        header_buf = 0;

        return (ssize_t)HDR_SZ;
    }

    // Returns true if a full header (or EOS) is available without consuming it.
    bool peek() {
        uint64_t szbe = 0;
        const ssize_t r = recv(fd, (char*)&szbe, HDR_SZ, MSG_PEEK | MSG_DONTWAIT);
        if (r == 0) return true; // EOS is readable
        return (r == (ssize_t)HDR_SZ);
    }

    ssize_t receive(void* buff, size_t size) {
        size_t incoming_size = 0;
        if (probe(incoming_size, true) < 0) return -1;

        // Early fail: if payload size is greater then buffer size returns EMSGSIZE so that
        // a new receive can be made with the correct buffer size
        if (incoming_size > size) {
            MTCL_TCP_PRINT(100, "HandleTCP::receive EMSGSIZE: incoming (%zu) > buffer (%zu)\n", incoming_size, size);
            errno = EMSGSIZE;
            return -1;
        }

        RequestTCP* req = new RequestTCP();
        req->reinit(fd, buff, size, &probed, this, false, true);

        {
            std::lock_guard<std::mutex> lk(rcv_mutex);
            active_req_receive.push_back(req);
        }

        int ret = req->wait();
        ssize_t bytes_read = req->count();

        delete req; // The request goes back in the pool of available requests

        if (ret == 0) return bytes_read;
        if (errno == ECONNRESET) return 0;
        return -1;
    }

    ssize_t ireceive(void* buff, size_t size, RequestPool& r) {
        TcpRequestVector* tcp_vec = r._getInternalVector<TcpRequestVector>();
        RequestTCP* req = tcp_vec->allocate(false);

        if (!req) {
            errno = ENOMEM;
            return -1;
        }

        req->reinit(fd, buff, size, &probed, this, false, false);

        {
            std::lock_guard<std::mutex> lk(rcv_mutex);
            active_req_receive.push_back(req);
        }

        continue_rcv_req();
        return 0;
    }

    ssize_t ireceive(void* buff, size_t size, Request& r) {
        RequestTCP* req = new RequestTCP();

        if(!req) {
            return -1;
        }

        req->reinit(fd, buff, size, &probed, this, false, false);
        r.__setInternalR(req);

        {
            std::lock_guard<std::mutex> lk(rcv_mutex);
            active_req_receive.push_back(req);
        }

        if (continue_rcv_req() < 0) return -1;
        return 0;
    }

    /**
     * @brief Flushes excess payload data from the socket buffer.
     * If a user calls ireceive() with a buffer smaller than the incoming
     * payload (EMSGSIZE), or if not all the payload has been correctly received,
     * the unread bytes remain in the OS socket buffer.
     * We must "drain" (read and discard) these remaining bytes before we can safely
     * read the next message header. Failure to do so corrupts the MTCL stream protocol.
     * * @return true if progress was made draining, false if blocked (EAGAIN) or EOF.
     */
    bool do_drain() {
        std::lock_guard<std::mutex> lk(rcv_mutex);

        // Finish reading the header and save the payload size
        if (is_draining_header.load(std::memory_order_acquire)) {
            ssize_t nread = ::read(fd, (char*)&header_buf + header_read, HDR_SZ - header_read);
            if (nread < 0) return false; // EAGAIN o EINTR
            if (nread == 0) return false; // EOF

            header_read += nread;
            if (header_read == HDR_SZ) {
                size_t size = (size_t)be64toh(header_buf);
                drain_payload_remaining.store(size, std::memory_order_release);

                is_draining_header.store(false, std::memory_order_release);
                header_read = 0;
                header_buf =  0;
                probed = {false, 0};
            }
            return true;
        }

        // Drain the payload
        size_t remaining = drain_payload_remaining.load(std::memory_order_acquire);
        if (remaining > 0) {
            char discard_buf[65536];
            size_t to_read = std::min(remaining, sizeof(discard_buf));
            ssize_t nread = ::read(fd, discard_buf, to_read);

            if (nread > 0) {
                drain_payload_remaining.fetch_sub(nread, std::memory_order_release);
                return true;
            }
        }
        return false;
    }

    // Get the first receive request in the deque and make some progress
    int continue_rcv_req() {

        if (needs_drain()) {
            do_drain();
            return 0;
        }

        RequestTCP *head = nullptr;

        {
            std::lock_guard<std::mutex> lk(rcv_mutex);
            if (active_req_receive.empty()) return 1;
            head = active_req_receive.front();
        }

        int status = head->doWork();

        // if the request is completed/failed
        if (status != 0) {
            std::lock_guard<std::mutex> lk(rcv_mutex);
            if (!active_req_receive.empty() && active_req_receive.front() == head) {
                active_req_receive.pop_front();
            }

            if (status > 0)
                return 1;
            if (status < 0)
                return -1;
        }
        return 0;
    }

    // Get the first send request in the deque and make some progress
    int continue_send_req() {
        RequestTCP *head = nullptr;

        {
            std::lock_guard<std::mutex> lk(send_mutex);
            if (active_req_send.empty()) return 1;
            head = active_req_send.front();
        }

        int status = head->doWork();

        // if the request is completed/failed
        if (status != 0) {
            std::lock_guard<std::mutex> lk(send_mutex);
            if (!active_req_send.empty() && active_req_send.front() == head) {
                active_req_send.pop_front();
            }
            if (status > 0)
                return 1;
            if (status < 0)
                return -1;
        }
        return 0;
    }

    ~HandleTCP() {
        std::deque<RequestTCP*> rcv_copy;
        {
            std::lock_guard<std::mutex> lk(rcv_mutex);
            std::swap(rcv_copy, active_req_receive);
        }

        for (RequestTCP* req : rcv_copy) {
            req->invalidate_handle();
        }

        std::deque<RequestTCP*> snd_copy;

        {
            std::lock_guard<std::mutex> lk(send_mutex);
            std::swap(snd_copy, active_req_send);
        }

        for (RequestTCP* req : snd_copy) {
            req->invalidate_handle();
        }
    }
};

class ConnTcp : public ConnType {
protected:
    std::string address;
    int port;

    std::unordered_map<int, Handle*> connections; // Active connections for this Connector

    std::vector<struct pollfd> fds;

    int listen_sck;
#if !defined(NO_MTCL_MULTITHREADED)
    std::shared_mutex shm;
#endif

private:

    /**
     * @brief Initializes the main listening socket for this Handle
     *
     * @return int status code
     */
    int _init() {
        if ((listen_sck = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
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
        if (getaddrinfo(address.c_str(), std::to_string(port).c_str(),
                        &hints, &result) != 0){
			MTCL_TCP_PRINT(100, "ConnTcp::_init getaddrinfo errno=%d\n", errno);
            return -1;
        }

        bool ok = false;
        for (rp = result; rp != NULL; rp = rp->ai_next) {
            if (bind(listen_sck, rp->ai_addr, (int)rp->ai_addrlen) < 0) {
				MTCL_TCP_PRINT(100, "ConnTcp::_init bind errno=%d, continue\n", errno);
                continue;
            }
            ok = true;
            break;
        }
        freeaddrinfo(result);
        if (!ok) {
			MTCL_TCP_PRINT(100, "ConnTcp::_init bind loop exit with errno=%d\n", errno);
            return -1;
        }
        if (::listen(listen_sck, TCP_BACKLOG) < 0) {
            MTCL_TCP_PRINT(100, "ConnTcp::_init listen errno=%d\n", errno);
            return -1;
        }

        return 0;
    }

    bool remove_fd_from_vector(int target_fd) {
        for (size_t i = 0; i < fds.size(); ++i) {
            if (fds[i].fd == target_fd) {
                fds[i] = fds.back();
                fds.pop_back();
                return true;
            }
        }
        return false;
    }

public:
    ConnTcp() {};
    ~ConnTcp() {
        RequestTCP::destroy_pool();
    };

    int init(std::string) {
        // For clients who do just connect, the communication thread anyway calls
		// the update method, and we do not want to call the select function with
		// invalid fields.
        REMOVE_CODE_IF(std::unique_lock lock(shm));
        fds.clear();
        listen_sck = -1;
        return 0;
    }

    int listen(std::string s) {
        address = s.substr(0, s.find(":"));
        port = stoi(s.substr(address.length() + 1));
        if (this->_init())
            return -1;

        MTCL_TCP_PRINT(1, "listen to %s:%d\n", address.c_str(),port);

        // add the listen socket to the read set
        REMOVE_CODE_IF(std::unique_lock lock(shm));
        fds.clear();
        fds.push_back({listen_sck, POLLIN, 0});

        return 0;
    }

    void update() {

        int wait_time = (TCP_POLL_TIMEOUT > 0) ? std::max<int>(1, TCP_POLL_TIMEOUT / 1000) : 0;
        int nready = 0;

        // copy the fds to a temporary one
        std::vector<pollfd> local_fds;
        {
            REMOVE_CODE_IF(std::unique_lock lock(shm));
            if (fds.empty()) return;
            local_fds = fds;
        }

        switch (nready = poll(local_fds.data(), local_fds.size(), wait_time)) {
        case -1: {
            // NOTE: EBADF can happen because we may close an fd that is in the set
            if (errno==EBADF) {
                MTCL_TCP_PRINT(100, "ConnTcp::update poll ERROR: errno=EBADF\n");
                return;
            }
            MTCL_TCP_ERROR("ConnTcp::update poll ERROR: errno=%d -- %s\n", errno, strerror(errno));
        }
        case  0: return;
        }

        REMOVE_CODE_IF(std::unique_lock ulock(shm, std::defer_lock));

        for (size_t idx_index = 0; idx_index < local_fds.size() && nready > 0; idx_index++) {
            int idx = local_fds[idx_index].fd;

            // No events available for this fd
            if(local_fds[idx_index].revents == 0)
                continue;

            if(local_fds[idx_index].revents & (POLLERR | POLLHUP | POLLNVAL)) {
                HandleTCP* h_to_close = nullptr;

                REMOVE_CODE_IF(ulock.lock());

                auto it = connections.find(idx);
                if (it != connections.end()) {
                    h_to_close = static_cast<HandleTCP*>(it->second);
                } else
                    remove_fd_from_vector(idx); // handle already closed, just remove the fd

                REMOVE_CODE_IF(ulock.unlock());

                if(h_to_close)
                    setAsClosed(h_to_close, false);

                continue;
            }

            bool can_read = local_fds[idx_index].revents & POLLIN;
            bool can_write = local_fds[idx_index].revents & POLLOUT;

            if(can_read || can_write) {
                if (idx == this->listen_sck && can_read) {
                    int connfd = accept(this->listen_sck, (struct sockaddr*)NULL, NULL);
                    if (connfd == -1) {
                        MTCL_TCP_ERROR("ConnTcp::update accept ERROR: errno=%d -- %s\n", errno, strerror(errno));
                        return;
                    }

#ifdef MTCL_DISABLE_NAGLE
                    int flag = 1;
                    if (setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY,
                                   (char *)&flag, sizeof(int)) < 0) {
                        MTCL_TCP_ERROR("ConnTcp::update setsockopt ERROR: errno=%d -- %s\n", errno, strerror(errno));
                        return;
                    }
#endif
                    REMOVE_CODE_IF(ulock.lock());

                    HandleTCP* new_handle = new HandleTCP(this, connfd);
                    connections[connfd] = new_handle;

                    REMOVE_CODE_IF(ulock.unlock());

                    addinQ(true, new_handle);

                    // set fd non blocking
                    set_nonblocking(connfd);
                } else {
                    REMOVE_CODE_IF(ulock.lock());

                    auto it = connections.find(idx);

                    // if handle has active requests then try to advance them
                    if (it != connections.end()) {
                        HandleTCP* h = static_cast<HandleTCP*>(it->second);
                        if(!h->is_yielded) {
                            REMOVE_CODE_IF(ulock.unlock());
                            continue;
                        }

                        // check requests status
                        bool active_recv = h->has_active_receive_req();
                        bool active_send = h->has_active_send_req();

                        // new request to be received -> give back control
                        if(can_read && !active_recv) {
                            for(size_t j = 0; j < fds.size(); ++j) {
                                if(fds[j].fd == idx) {
                                    fds[j] = fds.back();
                                    fds.pop_back();
                                    break;
                                }
                            }

                            h->is_yielded.store(false, std::memory_order_release);
                            REMOVE_CODE_IF(ulock.unlock());

                            addinQ(false, h);
                            continue;
                        }

                        if (can_read && h->has_active_receive_req()) {
                            h->continue_rcv_req();
                        }
                        if (can_write && h->has_active_send_req()) {
                            h->continue_send_req();
                        }

                        // Updates ready connections and removes from listening
                        active_send = h->has_active_send_req();
                        if (!active_send) {
                            // Stop listening for write on this fd
                            for(auto& orig_fd : fds) {
                                if(orig_fd.fd == idx) orig_fd.events = orig_fd.events & ~POLLOUT;
                            }
                        }
                    }
                    REMOVE_CODE_IF(ulock.unlock());
                }
                --nready;
            }
        }
    }

    // URL: host:prot || label: user string
    Handle* connect(const std::string& address, int retry, unsigned timeout_ms) {

        int fd = internal_connect(address, retry, timeout_ms);
        if (fd == -1) {
            return nullptr;
        }

        if (set_nonblocking(fd) < 0) {
            close(fd);
            return nullptr;
        }

#ifdef MTCL_DISABLE_NAGLE
        int flag = 1;
        if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int)) < 0){
			MTCL_TCP_ERROR("ConnTcp::connect setsockopt ERROR: errno=%d -- %s\n", errno, strerror(errno));
            return nullptr;
        }
#endif

        HandleTCP *handle = new HandleTCP(this, fd);
        {
            REMOVE_CODE_IF(std::unique_lock lock(shm));
            connections[fd] = handle;
        }
        return handle;
    }

    void notify_close(Handle* h, bool close_wr = true, bool close_rd = true) {
        HandleTCP *handle = static_cast<HandleTCP*>(h);
        if (close_wr && handle->fd != -1) {
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
        if (close_rd) {
            int fd = handle->fd;
            if (fd == -1) return;

            shutdown(handle->fd, SHUT_RD);

            {
                REMOVE_CODE_IF(std::unique_lock lock(shm));
                connections.erase(fd);
                remove_fd_from_vector(fd);
            }

            if (close_wr) {
                close(fd);
                handle->fd = -1;
            }
        }
    }

    void notify_yield(Handle* h) override {
        HandleTCP* handle = static_cast<HandleTCP*>(h);
        int fd = handle->fd;
        if (fd == -1) return;
        if (handle->isClosed()) return;

        if (handle->is_yielded.exchange(true, std::memory_order_release)) {
            return; // flag was already true
        }

        REMOVE_CODE_IF(std::unique_lock lock(shm));

        short events = POLLIN;
        if (handle->has_active_send_req()) {
            events |= POLLOUT;
        }

        bool found = false;
        for (auto& pfd : fds) {
            if (pfd.fd == fd) {
                pfd.events |= events;
                found = true;
                break;
            }
        }

        if (!found) {
            fds.push_back({fd, events, 0});
        }
    }

    void end(bool blockflag = false) {
        std::unordered_map<int, Handle*> modified_connections;

        {
            REMOVE_CODE_IF(std::shared_lock lock(shm));
            modified_connections = connections;
        }

        for (auto& [fd, h] : modified_connections) {
            setAsClosed(h, blockflag);
        }
    }

    bool isSet(int fd) {
        REMOVE_CODE_IF(std::shared_lock s(shm));
        for(auto x : fds){
            if(x.fd == fd)
                return true;
        }
        return false;
    }
};



/**
 * Get the header and then call readn to start reading the payload.
 */
inline int RequestTCP::receiveMsg() {
    // Read the header if it has not been read yet
    if (probed && !probed->first) {
        size_t sz(0);
        ssize_t pr = parent_handle->probe(sz, false);
        if (pr < 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                request_state.store(State::IDLE, std::memory_order_release);
                return 0;
            } else {
                request_state.store(State::FAILED, std::memory_order_release);
                return -1;
            }
        }
        if (pr == 0) {
            errno = ECONNRESET;
            error_code = ECONNRESET;
            request_state.store(State::FAILED, std::memory_order_release);
            return -1;
        }
    }

    if (probed) payload_total = probed->second;
    // empty payload -> Read completed
    if (payload_total == 0) {
        if (probed) *probed = {false, 0};
        request_state.store(State::COMPLETED, std::memory_order_release);
        return 1;
    }

    // Drain the message if the payload is greater than the user buffer
    if ((payload_total > user_buffer_size)) {

        MTCL_TCP_PRINT(100, "HandleTCP::ireceive EMSGSIZE, buffer too small\n");

        char drain_chunk[65536];

        size_t payload_read_cpy = payload_read.load(std::memory_order_acquire);

        if(payload_read_cpy<payload_total){
            ssize_t nleft = std::min((payload_total-payload_read_cpy), sizeof(drain_chunk));
            size_t curr_off = 0;
            int res = readn(drain_chunk, nleft, curr_off); // read the buffer inside the drain chunk
            payload_read_cpy += curr_off;
            if(res == -1) {
                if (probed && probed->first) *probed = {false, 0};

                payload_read.store(payload_read_cpy, std::memory_order_release);
                request_state.store(State::FAILED, std::memory_order_release);
                errno = EMSGSIZE;
                error_code = EMSGSIZE;
                return -1;
            }

            // Return 0 until is all drained
            if (payload_read_cpy < payload_total) {
                payload_read.store(payload_read_cpy, std::memory_order_release);
                request_state.store(State::IDLE, std::memory_order_release);
                return 0;
            }
        }

        // return error after drain
        errno = EMSGSIZE;
        error_code = EMSGSIZE;
        if (probed) *probed = {false, 0};
        payload_read.store(payload_read_cpy, std::memory_order_release);
        request_state.store(State::FAILED, std::memory_order_release);
        return -1;
    }

    // read the payload
    size_t current_read = payload_read.load(std::memory_order_acquire);
    int res = readn(user_buffer, payload_total, current_read);
    payload_read.store(current_read, std::memory_order_release);
    if (res == 1) {
        if (probed && probed->first) *probed = {false, 0};
        request_state.store(State::COMPLETED, std::memory_order_release);
    }
    if (res == -1) {
        if (probed && probed->first) *probed = {false, 0};
        request_state.store(State::FAILED, std::memory_order_release);
    }
    if (res == 0) request_state.store(State::IDLE, std::memory_order_release);
    return res;
}

inline int RequestTCP::sendMsg() {
    int res = writevn(iov, 2);

    // payload written contains only the byte written from the payload, not the header
    payload_written.store(user_buffer_size - iov[1].iov_len, std::memory_order_release);

    if (res < 0) {
        request_state.store(State::FAILED, std::memory_order_release);
        return -1;
    } else if(res == 0) {
        request_state.store(State::IDLE, std::memory_order_release);
        return 0;
    }

    // Request completed
    request_state.store(State::COMPLETED, std::memory_order_release);

    return 1;
}

inline void RequestTCP::release() {
    delete this;
}

inline int RequestTCP::test(int& result) {
    int res = check_status(); // check if it's already completed
    if (res != 0) {
        result = 1;
        if (res == -1) { errno = error_code; return -1; }
        return 0;
    }

    // advance the requests
    if (parent_handle != nullptr) {
        if (operation == OpType::RECEIVE) {
            if((parent_handle->continue_rcv_req()) == 0)
                parent_handle->continue_send_req();
        } else {
            if((parent_handle->continue_send_req()) == 0)
                parent_handle->continue_rcv_req();
        }
    }

    // return current status
    res = check_status();
    if (res != 0) {
        result = 1;
        if (res == -1) {
            errno = error_code;
            return -1;
        }
        return 0;
    }
    result = 0;
    return 0;
}

inline void RequestTCP::invalidate_handle() {
    while (true) {
        State expected = State::IDLE;
        if (request_state.compare_exchange_strong(expected, State::BUSY, std::memory_order_acquire)) {

            // if it's a receive and it's not finished, save the state to drain the remaining bytes
            if (parent_handle != nullptr) {
                if (operation == OpType::RECEIVE) {
                    std::lock_guard<std::mutex> lk(parent_handle->rcv_mutex);

                    if (!parent_handle->active_req_receive.empty() &&
                        parent_handle->active_req_receive.front() == this) {

                        bool interrupted_header = false;
                        size_t missing_payload = 0;
                        // header half read
                        if (probed && !probed->first && parent_handle->header_read > 0) {
                            interrupted_header = true;
                        }
                        // payload not fully read
                        else if (payload_total > 0 && payload_read.load(std::memory_order_acquire) < payload_total) {
                            missing_payload = payload_total - payload_read.load(std::memory_order_acquire);
                        }
                        else if (payload_total == 0 && probed && probed->first) {
                            missing_payload = probed->second;
                        }

                        // save the bytes it needs to drain before the next receive
                        if (interrupted_header || missing_payload > 0) {
                            if (interrupted_header) {
                                parent_handle->is_draining_header.store(true, std::memory_order_release);
                            } else {
                                parent_handle->drain_payload_remaining.store(missing_payload, std::memory_order_release);
                            }

                            if (probed) *probed = {false, 0};
                            parent_handle->header_read = 0;
                            parent_handle->header_buf = 0;
                        }
                    }

                    // remove the request from the active deque
                    auto& deque = parent_handle->active_req_receive;
                    auto it = std::find(deque.begin(), deque.end(), this);
                    if (it != deque.end()) {
                        deque.erase(it);
                    }
                } else { // if it's a send request, just remove it from the deque
                    std::lock_guard<std::mutex> lk(parent_handle->send_mutex);
                    auto& deque = parent_handle->active_req_send;
                    auto it = std::find(deque.begin(), deque.end(), this);
                    if (it != deque.end()) {
                        deque.erase(it);
                    }
                }
            }

            parent_handle = nullptr;
            error_code = EBADF;
            request_state.store(State::FAILED, std::memory_order_release);
            return;
        }

        // if it is completed/failed, remove from deque
        if (expected == State::COMPLETED || expected == State::FAILED) {
            removeFromDeque();
            return;
        }
    }
}

// if wait() fails, this struct
// allows the fd to be set non blocking again
struct ScopedBlockingGuard {
    int fd;
    bool ok;
    ScopedBlockingGuard(int _fd) : fd(_fd) {
        ok = (set_blocking(fd) != -1);
    }
    ~ScopedBlockingGuard() {
        if (ok) set_nonblocking(fd);
    }
};

// if io thread is not active, the fd can be set to blocking
#ifdef SINGLE_IO_THREAD

inline int RequestTCP::wait() {
    int res = check_status(); // check if it's already completed
    if (res == 1) return 0;
    if (res == -1) {
        errno = error_code;
        return -1;
    }

    if (parent_handle == nullptr) {
        errno = EBADF;
        return -1;
    }

    int fd = parent_handle->fd;

    // set blocking fd
    ScopedBlockingGuard guard(fd);
    if (!guard.ok) return -1;

    blocking = true;

    while (true) {
        if (parent_handle == nullptr) {
            errno = EBADF;
            return -1;
        }

        if (operation == OpType::RECEIVE)
            parent_handle->continue_rcv_req();
        else
            parent_handle->continue_send_req();

        res = check_status();
        if (res == 1) return 0;
        if (res == -1) {
            errno = error_code;
            return -1;
        }
    }
}

/** if io thead is active, a blocking fd can cause
 * the io thread to get stuck in a single read,
 * so a progressive backoff strategy is used.
 */
#else

inline int RequestTCP::wait() {
    int res = check_status();
    if (res == 1) return 0;
    if (res == -1) {
        errno = error_code;
        return -1;
    }

    if (parent_handle == nullptr) {
        errno = EBADF;
        return -1;
    }

    size_t spins = 0;

    while (true) {
        if (parent_handle == nullptr) {
            errno = EBADF;
            return -1;
        }

        // advance all requests of the same type of the one that the wait was called on
        if (operation == OpType::RECEIVE) {
            if((parent_handle->continue_rcv_req()) == 0)
                parent_handle->continue_send_req();
        } else {
            if((parent_handle->continue_send_req()) == 0)
                parent_handle->continue_rcv_req();
        }

        res = check_status();
        if (res == 1) return 0;
        if (res == -1) {
            errno = error_code;
            return -1;
        }

        // PROGRESSIVE BACKOFF STRATEGY
        // We are waiting for the IO thread to fulfill our request.
        // - Phase 1 (0-100): CPU relax. Optimize for ultra-low latency (cache-hit networking).
        // - Phase 2 (100-1000): yield.
        // - Phase 3 (>1000): Sleep 50us. The network is slow; stop burning CPU cycles and save power.
        if (spins < 100) {
            MTCL::mtcl_cpu_relax();
        } else if (spins < 1000) {
            std::this_thread::yield();
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        spins++;
    }
}
#endif

// remove a request from the deque
inline void RequestTCP::removeFromDeque() {
    if (parent_handle == nullptr)
        return;

    if (operation == OpType::RECEIVE) {
        std::lock_guard<std::mutex> lk(parent_handle->rcv_mutex);
        auto& deque = parent_handle->active_req_receive;
        auto it = std::find(deque.begin(), deque.end(), this);
        if (it != deque.end())
            deque.erase(it);
    } else {
        std::lock_guard<std::mutex> lk(parent_handle->send_mutex);
        auto& deque = parent_handle->active_req_send;
        auto it = std::find(deque.begin(), deque.end(), this);
        if (it != deque.end())
            deque.erase(it);
    }
}


} // namespace
