#pragma once

/**
 * UCX backend based on UCP STREAM.
 *
 * UCP STREAM provides a byte-stream semantics (TCP-like), therefore MTCL
 * implements message framing in userspace (header + payload). 
 *
 * It is important not to desynchronize the stream when multiple asynch
 * receives are posted concurrently.
 * 
 * TODO: introduce a policy to enforce ordering of receive in case 
 *       wait() is called by the user out of ireceive post order.
 */

#include <iostream>
#include <map>
#include <string.h>
#include <shared_mutex>
#include <vector>
#include <algorithm>
#include <deque>

#include <unistd.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <ucp/api/ucp.h>

#include "../handle.hpp"
#include "../protocolInterface.hpp"
#include "../utils.hpp" 
#include "../config.hpp"

namespace MTCL {

class HandleUCX;

/**
 * Per-request completion record used as UCP user_data for callbacks.
 * - complete: set to 1 by the callback when the operation completes
 * - length:   number of bytes completed (when relevant)
 * - status:   UCX completion status
 */	
typedef struct test_req {
    int complete = 0;
	size_t length = 0;
	ucs_status_t status = UCS_OK;
} test_req_t;

/**
 * requestUCX is the internal async request used by isend() (and potentially other ops).
 *
 * Ownership rules:
 * - content points to a dynamically allocated ucp_dt_iov_t[] used for UCP_DATATYPE_IOV
 * - iov[0].buffer points to a dynamically allocated header (size in BE)
 * - UCX may access these buffers until the request completes; therefore we must NOT
 *   free them until completion (or after cancellation + progress-to-completion)
 */	
struct requestUCX : public request_internal {
    friend class HandleUCX;
    friend class ConnRequestVectorUCX;
    void* content;
    ucs_status_ptr_t request;
    test_req_t ctx{};
    ucp_worker_h ucp_worker;
	size_t  expected = 0;
	ssize_t got = -1;
    
    requestUCX(void* content, ucp_worker_h w, size_t exp) :
		content(content), ucp_worker(w), expected(exp) {}
	
	// test() does not call ucp_worker_progress() here:
	//  the progress engine is driven by RequestPool via make_progress()
	// and/or external progression
    int test(int& result){
        // Operation completed immediately, callback is not called!
        if(request == NULL) {
			got = (ssize_t)expected;
            result = 1; // success
            return 0;   
        }
        if(ctx.complete == 0) {
            result = 0;
            return 0;
        }
        result = 1;
		if (ctx.status != UCS_OK) {
            errno = ECOMM;
            ucp_request_free(request);
            request = NULL;
            return -1;
        }
        ucp_request_free(request);
        request = NULL;
        return UCS_OK;
    }

    int make_progress(){
        ucp_worker_progress(ucp_worker);
        return 0;
    }

	ssize_t count() const override { return got; }
	
    ~requestUCX(){
		// If the request is still in-flight and the user destroys the Request(Pool)
        // without waiting, we must not free the iov/header buffers while UCX may
        // still be accessing them. Try to cancel the request and progress until
		// completion, then free.
        if (request && !UCS_PTR_IS_ERR(request)) {
            if (!ctx.complete) {
                ucp_request_cancel(ucp_worker, request);
                while (!ctx.complete) {
                    ucp_worker_progress(ucp_worker);
                }
            }
            ucp_request_free(request);
            request = nullptr;
        }
		if (content) {
            auto* iov = static_cast<ucp_dt_iov_t*>(content);
            delete static_cast<uint64_t*>(iov[0].buffer); // delete the header
            free(iov);
			content = nullptr;
		}
    }
};

/**
 * @brief Variable-length receive request for UCX stream-based transports
 *
 * UCX stream endpoints behave like a byte stream (TCP-like) and do not preserve
 * message boundaries. MTCL therefore frames each message as:
 *
 *   [uint64_t payload_size][payload bytes]
 *
 * requestUCXRecvVar implements an MPI-like Irecv where the user provides a
 * buffer capacity (not the exact message size). The request progresses in two
 * phases:
 *
 *  1) Receive exactly the fixed-size header to discover the incoming payload size.
 *  2) Receive the payload:
 *     - If payload_size <= capacity, copy the whole payload into the user buffer
 *       and complete with count = payload_size.
 *     - If payload_size > capacity, no data is copied ato the user buffer.
 *       The whole message is drained from the stream into a temporary buffer to keep 
 *       the stream aligned. The request completes only after draining, returning 
 *       an error (EMSGSIZE) but still records the real message size in count() so 
 *       the caller can react.
 *
 * This class exists to prevent stream desynchronization that would occur if a
 * single WAITALL receive were posted with a buffer larger than the actual message,
 * which would consume bytes belonging to subsequent messages and corrupt framing.
 */
class requestUCXRecvVar : public request_internal {
    ucp_ep_h endpoint;
    ucp_worker_h worker;
    void* user_buff;
    size_t cap;

    uint64_t hdr_be = 0;
    size_t msg_size = 0;

    size_t out_len = 0;
    ucs_status_ptr_t req = nullptr;
    test_req_t ctx{};
    ucp_request_param_t param{};

    size_t remaining = 0;
    std::vector<char> discard;

    enum class Stage { HDR, PAYLOAD, DRAIN, DONE };
    Stage stage = Stage::HDR;

    ssize_t got = -1;
    int err = 0;

	/**
     * This is meant to support per-handle gating (only 1 framed recv "armed" at a time)
     * without modifying RequestPool. The callback typically receives a HandleUCX* as done_ctx.
     *
     * IMPORTANT: currently the code calls notify_done() only in some early-error paths.                             // <-----------------
     * If you later rely on this hook for gating, ensure notify_done() is called on ALL DONE transitions.
     */
    using done_cb_t = void (*)(void* done_ctx, requestUCXRecvVar* req);
    done_cb_t done_cb  = nullptr;
    void* done_ctx     = nullptr; // callback context (e.g., HandleUCX*)
    bool armed         = false;   // if true we start receiving on this request
    bool done_notified = false;

    void notify_done() {
        if (!done_notified && done_cb) {
            done_notified = true;
            done_cb(done_ctx, this);
        }
    }

    static void recv_cb(void *request, ucs_status_t status, size_t length, void *user_data) {
        auto* c = reinterpret_cast<test_req_t*>(user_data);
        c->status = status;
        c->length = length;
        c->complete = 1;
    }
	
	// Post a STREAM receive of exactly 'len' bytes using the WAITALL flag, which ensures
	// UCX completes only when len bytes are available in the stream.
    void start_recv(void* buf, size_t len) {
        ctx.complete = 0;
        ctx.length = 0;
        ctx.status = UCS_OK;

        param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                             UCP_OP_ATTR_FIELD_DATATYPE |
                             UCP_OP_ATTR_FIELD_USER_DATA |
                             UCP_OP_ATTR_FIELD_FLAGS;
        param.datatype = ucp_dt_make_contig(1);
        param.user_data = &ctx;
        param.flags = UCP_STREAM_RECV_FLAG_WAITALL;
        param.cb.recv_stream = recv_cb;

        out_len = 0;
        req = ucp_stream_recv_nbx(endpoint, buf, len, &out_len, &param);

        if (req == nullptr) { // immediate completion, callback not called
            ctx.complete = 1;
            ctx.length = out_len;
        } else if (UCS_PTR_IS_ERR(req)) {
            err = EINVAL;
            stage = Stage::DONE;
            notify_done();
        }
    }

    void free_req_if_needed() {
        // Only free a UCX request object after completion.
		// Freeing a request that is still in progress is UB
        if (req && !UCS_PTR_IS_ERR(req) && ctx.complete) {
            ucp_request_free(req);
        }
        req = nullptr;
    }

    void cancel_req_if_pending() {
		// Best-effort cancellation used by the destructor to avoid leaving in-flight
		// operations behind if the user destroys the pool early.
		//
		// TODO: in pathological cases this could spin indefinitely; 
        // add a bounded progress loop + warning message.
        if (req && !UCS_PTR_IS_ERR(req) && !ctx.complete) {
            ucp_request_cancel(worker, req);
            while (!ctx.complete) {     
                ucp_worker_progress(worker);
            }
        }
    }
	// Drive the internal state machine:
	//  HDR    -> PAYLOAD (if fits) or DRAIN (if too large) or DONE (EOS)
	//  PAYLOAD-> DONE
	//  DRAIN  -> DONE after draining remaining bytes
	//
	// It assumes the current UCX recv is marked complete (ctx.complete==1).
    void advance_state() {
        if (stage == Stage::DONE) return;
        if (!ctx.complete) return;
        if (ctx.status != UCS_OK) {
            err = ECOMM;
            free_req_if_needed();
            stage = Stage::DONE;
            notify_done();
            return;
        }
        if (stage == Stage::HDR) {
            free_req_if_needed();

            msg_size = static_cast<size_t>(be64toh(hdr_be));

            if (msg_size == 0) { // EOS
                got = 0;
                stage = Stage::DONE;
                notify_done();
                return;
            }
            if (msg_size <= cap) {
                stage = Stage::PAYLOAD;
                start_recv(user_buff, msg_size);
                return;
            }

            err = EMSGSIZE;
            remaining = msg_size;
			// Too large payload: read and discard the remaining bytes to keep the stream aligned
			// User receives EMSGSIZE, but the stream stays consistent for subsequent reads.
            discard.resize(std::min<size_t>(remaining, UCX_DRAIN_CHUNK_SIZE));
            stage = Stage::DRAIN;
            start_recv(discard.data(), discard.size());
            return;
        }
        if (stage == Stage::PAYLOAD) {
            free_req_if_needed();
            got = (ssize_t)(ctx.length ? ctx.length : msg_size);
            stage = Stage::DONE;
            notify_done();
            return;
        }
        if (stage == Stage::DRAIN) {
            free_req_if_needed();

            size_t consumed = ctx.length ? ctx.length : discard.size();
            if (remaining >= consumed) remaining -= consumed;
            else remaining = 0;

            if (remaining == 0) {
                got = (ssize_t)msg_size;
                stage = Stage::DONE;
                notify_done();
                return;
            }

            discard.resize(std::min<size_t>(remaining, UCX_DRAIN_CHUNK_SIZE));
            start_recv(discard.data(), discard.size());
        }
    }

public:
    // Arm this request (i.e., start UCX recv)
    void arm_request() {
        if (armed) return;
        armed = true;

        if (stage == Stage::DONE) { notify_done(); return; }

        if (stage == Stage::HDR) {
            start_recv(&hdr_be, sizeof(uint64_t));
            return;
        }

        if (stage == Stage::PAYLOAD) {
            start_recv(user_buff, msg_size);
            return;
        }

        if (stage == Stage::DRAIN) {
            start_recv(discard.data(), discard.size());
            return;
        }
    }

    // Used when the message header has already been consumed by HandleUCX::probe()
    // and the payload length is already known.
    requestUCXRecvVar(ucp_ep_h ep, ucp_worker_h w, void* buff, size_t capacity, size_t known_msg_size,
                      done_cb_t cb=nullptr, void* ctx=nullptr)
        : endpoint(ep), worker(w), user_buff(buff), cap(capacity), done_cb(cb), done_ctx(ctx) {
        msg_size = known_msg_size;

        if (msg_size == 0) {
            got = 0;
            stage = Stage::DONE;
            return;
        }
        if (msg_size <= cap) {
            stage = Stage::PAYLOAD;
            return;
        }
        err = EMSGSIZE;
        remaining = msg_size;
        discard.resize(std::min<size_t>(remaining, UCX_DRAIN_CHUNK_SIZE));
        stage = Stage::DRAIN;
    }

    // Used when we have only the buffer capacity and don't know the actual size
    requestUCXRecvVar(ucp_ep_h ep, ucp_worker_h w, void* buff, size_t capacity,
                      done_cb_t cb=nullptr, void* ctx=nullptr)
        : endpoint(ep), worker(w), user_buff(buff), cap(capacity), done_cb(cb), done_ctx(ctx) {
        stage = Stage::HDR;
    }

    int test(int& result) override {
        if (!armed) { result = 0; return 0; }
        advance_state();

        if (stage == Stage::DONE) {
            result = 1;
            notify_done();
            if (err) { errno = err; return -1; }
            return 0;
        }

        result = 0;
        return 0;
    }

    int wait() override {
		// TODO: instead of returning EDEADLK we can drive
		// the current front receive(s) to completion until
		// the handle becomes the first one.
		if (!armed) { errno = EDEADLK; return -1; }
        int done = 0;
        while (true) {
            if (test(done) < 0) return -1;
            if (done) return 0;
			make_progress();
        }
    }

    int make_progress() override {
        ucp_worker_progress(worker);
        return 0;
    }

    ssize_t count() const override {
        if (stage != Stage::DONE) return -1;
        return got;
    }

    ~requestUCXRecvVar() override {
        cancel_req_if_pending();
        free_req_if_needed();
        notify_done();
    }
};
	
class ConnRequestVectorUCX : public ConnRequestVector {
    friend class HandleUCX;
    std::vector<request_internal*> requests;
public:
    ConnRequestVectorUCX(size_t sizeHint = 1){
        requests.reserve(sizeHint);
    }

    bool testAll(){
        int res = 0;
		// Make progress on all workers involved 
        for (auto r : requests) r->make_progress();
        for (auto r : requests){
            r->test(res);
            if (!res) return false;
        }
        return true;
    }

    void waitAll(){
        int res = 0;
        while(true){
            bool allCompleted = true;
            for(auto r : requests){
                r->test(res);
                if (!res) {
                    allCompleted = false;
                    r->make_progress();
                }
            }
            if (allCompleted) return;
        }
    }

    void reset(){
        for(auto r : requests) delete r;
        requests.clear();
    }
};

class HandleUCX : public Handle {
    /**
     * The callback on the sending side, which is invoked after finishing sending
     * the message.
     */
    static void send_cb(void *request, ucs_status_t status, void *user_data) {
		((test_req_t*)user_data)->status = status;
        ((test_req_t*)user_data)->complete = 1;
    }
    /**
     * The callback on the receiving side, which is invoked upon receiving the
     * stream message.
     */
    static void stream_recv_cb(void *request, ucs_status_t status, size_t length, void *user_data) {
		((test_req_t*)user_data)->status = status;
		((test_req_t*)user_data)->length = length;
		((test_req_t*)user_data)->complete = 1;
    }

	/**
	 * Asynchronous probe state.
     * probe() is implemented by posting a 8-byte WAITALL receive for the header.
     * If the user calls probe(blocking=false) repeatedly, we keep the pending UCX request
     * in pending_probe_req and complete it later without losing bytes from the stream.
     */
	ucs_status_ptr_t     pending_probe_req = nullptr;
	test_req_t           pending_probe_ctx;
	ucp_request_param_t  pending_probe_param{};
	uint64_t             pending_probe_word = 0; // probe's stable buffer
	size_t               pending_probe_res  = 0;

	
	// UCX STREAM is a byte-stream: multiple concurrent recv(WAITALL) on the same
	// endpoint may split the stream and break MTCL's [hdr][payload] framing.
	// We therefore allow multiple ireceive() to be enqueued, but we arm at most
	// one receive request at a time per HandleUCX.
	std::deque<requestUCXRecvVar*> rx_queue;

	static void rx_done_cb(void* ctx, requestUCXRecvVar* r) {
		static_cast<HandleUCX*>(ctx)->on_rx_done(r);
	}

	void on_rx_done(requestUCXRecvVar* r) {
		if (!rx_queue.empty() && rx_queue.front() == r) {
			rx_queue.pop_front();
			if (!rx_queue.empty()) rx_queue.front()->arm_request();
			return;
		}
		// Defensive removal in case of out-of-order completion/cancellation.
		for (auto it = rx_queue.begin(); it != rx_queue.end(); ++it) {
			if (*it == r) {
				rx_queue.erase(it);
				break;
			}
		}
	}

	void enqueue_rx(requestUCXRecvVar* r) {
		rx_queue.push_back(r);
		if (rx_queue.size() == 1) {
			r->arm_request();
		}
	}

protected:
	
    static void fill_request_param(test_req_t* ctx, ucp_request_param_t* param, bool is_iov) {
        ctx->complete = 0;
		ctx->length   = 0;
		ctx->status   = UCS_OK;
        param->op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                              UCP_OP_ATTR_FIELD_DATATYPE |
                              UCP_OP_ATTR_FIELD_USER_DATA;
		
        param->datatype     = is_iov ? UCP_DATATYPE_IOV : ucp_dt_make_contig(1);
        param->user_data    = ctx;
    }

    ucs_status_t request_wait(ucs_status_ptr_t l_request, test_req_t* ctx,
							  char* operation, bool blocking) {
        ucs_status_t status = UCS_OK;

        // Operation completed immediately, callback is not called!
        if(l_request == NULL) {
            return status;   // OK!
        }

        if(UCS_PTR_IS_ERR(l_request)) {
            status = UCS_PTR_STATUS(l_request);
            MTCL_UCX_PRINT(100, "HandleUCX::request_wait UCX_%s request error (%s)\n",
						   operation, ucs_status_string(status));
            return status;
        }
        if(!blocking) {
            ucp_worker_progress(ucp_worker);
            if(ctx->complete == 0) return UCS_INPROGRESS;
        }

        while(ctx->complete == 0) {
            ucp_worker_progress(ucp_worker);
        }
        status = ucp_request_check_status(l_request);
    
        if(status != UCS_OK) {
            MTCL_UCX_PRINT(100, "HandleUCX::request_wait UCX_%s status error (%s)\n",
						   operation, ucs_status_string(status));
        }
        return status;
    }
	// Receive exactly 'size' bytes from the UCX stream (WAITALL).
	// This is used only when the exact message size is already known (after probe).
	// For variable-length receives, use requestUCXRecvVar.
    ssize_t receive_internal(void* buff, size_t size, bool blocking) {
        size_t res = 0;
        test_req_t ctx{};
        ucp_request_param_t param{};
		fill_request_param(&ctx, &param, false);
		param.op_attr_mask  |= UCP_OP_ATTR_FIELD_FLAGS;
		param.flags          = UCP_STREAM_RECV_FLAG_WAITALL;
		param.cb.recv_stream = stream_recv_cb;
		ucs_status_ptr_t request   = ucp_stream_recv_nbx(endpoint, buff, size, &res, &param);

        ucs_status_t status;
        if((status = request_wait(request, &ctx, (char*)"receive_internal", blocking)) != UCS_OK) {
            if(status == UCS_INPROGRESS) {
                errno = EWOULDBLOCK;
                return -1;
            }
			int rc = -1;
			if(status == UCS_ERR_CONNECTION_RESET) {
                res = 0;
				rc  = 0;
            } else {
                errno = EINVAL;
            }
            return rc;
        }
        if (request) ucp_request_free(request);
        return (ssize_t) res;
    }
	// Asynchronous send with framing:
	//   [size_t payload_size (BE)][payload bytes]
	//
	// We build an IOV array:
	//   iov[0] = pointer to heap-allocated header
	//   iov[1] = pointer to user buffer (if size != 0)
	//
	// requestUCX takes ownership of iov and header and frees them in its destructor
	// after completion/cancellation.
    bool isend_internal(const void* buff, size_t size, requestUCX*& rq) {
		const int niov = (size == 0) ? 1 : 2;

		auto* iov = static_cast<ucp_dt_iov_t*>(calloc(niov, sizeof(ucp_dt_iov_t)));
		if (!iov) { errno = ENOMEM; rq = nullptr; return false; }

		auto* sz = new uint64_t(htobe64((uint64_t)size));
		if (!sz) { free(iov); errno = ENOMEM; rq = nullptr; return false;}
		iov[0].buffer = sz;
		iov[0].length = sizeof(uint64_t);

		if (size != 0) {
			iov[1].buffer = const_cast<void*>(buff);
			iov[1].length = size;
		}
		rq = new requestUCX(iov, ucp_worker, size);
		if (!rq) {  errno = ENOMEM; return false; }
		
		ucp_request_param_t param{};
		param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                             UCP_OP_ATTR_FIELD_DATATYPE |
                             UCP_OP_ATTR_FIELD_USER_DATA;
		param.datatype     = UCP_DATATYPE_IOV;
		param.user_data    = &rq->ctx;
		param.cb.send      = send_cb;

		rq->request = ucp_stream_send_nbx(endpoint, iov, niov, &param);

		if (rq->request == NULL) {
			rq->got = size;
			return true;       // immediate completion
		}

		if (UCS_PTR_IS_ERR(rq->request)) {
			ucs_status_t status = UCS_PTR_STATUS(rq->request);
			MTCL_UCX_PRINT(100, "HandleUCX::isend request error (%s)\n", ucs_status_string(status));
			delete rq;      // ~requestUCX will free the header and iov
			rq = nullptr;
			errno = (status == UCS_ERR_NO_MEMORY) ? ENOMEM : EIO;
			return false;
		}
		return true;
	}

public:
    std::atomic<bool> already_closed {false};
    ucp_ep_h     endpoint;
    ucp_worker_h ucp_worker;

	// Note: closed_wr/closed_rd track the local view of stream shutdown.
    // - closed_wr: we have sent EOS (size==0 frame) or shutdown write side
    // - closed_rd: we have observed EOS from peer 
    //
    // These flags are used by the generic ConnType::setAsClosed() logic.
    std::atomic<bool> closed_wr{false};
    std::atomic<bool> closed_rd{false};

    size_t test_probe = 42;

    HandleUCX(ConnType* parent, ucp_ep_h endpoint, ucp_worker_h worker) :
		Handle(parent), endpoint(endpoint), ucp_worker(worker) {}

    ssize_t send(const void* buff, size_t size) {
		const int niov = (size == 0) ? 1 : 2;
		uint64_t sz = htobe64((uint64_t)size);
    
        ucp_dt_iov_t iov[2];
        iov[0].buffer = &sz;
        iov[0].length = sizeof(sz);
		if (size) {
			iov[1].buffer = const_cast<void*>(buff);
			iov[1].length = size;
		}

        ucp_request_param_t param{};
        test_req_t ctx{};

        fill_request_param(&ctx, &param, true);
        param.cb.send = send_cb;
        ucs_status_ptr_t request_ = ucp_stream_send_nbx(endpoint, iov, niov, &param);
		ucs_status_t status;
		if((status = request_wait(request_, &ctx, (char*)"send", true)) != UCS_OK) {
			int res = -1;
			if(status == UCS_ERR_CONNECTION_RESET)
				errno = ECONNRESET;
			else
				errno = EINVAL;
			
			// request_wait() does not free UCX requests
			if (request_ && !UCS_PTR_IS_ERR(request_)) ucp_request_free(request_);
			return res;
		}
		// request_wait() does not free UCX requests
		if (request_ && !UCS_PTR_IS_ERR(request_)) ucp_request_free(request_);
		
		return size;
    }
    ssize_t sendEOS() {
		// EOS is encoded as a framed message with size==0 (header only)
		return send(nullptr, 0);
    }
	
    ssize_t isend(const void* buff, size_t size, Request& r) {
		requestUCX* rq=nullptr;
		auto ret = isend_internal(buff, size, rq);
		if (ret) {
			if (rq) {
				r.__setInternalR(rq);
			}
			return 0;
		}
		return -1;
    }

    ssize_t isend(const void* buff, size_t size, RequestPool& r) {
		requestUCX* rq=nullptr;
		auto ret = isend_internal(buff, size, rq);
		if (ret) {
			if (rq) {
				r._getInternalVector<ConnRequestVectorUCX>()->requests.push_back(rq);
			}
			return 0;
		}
		return -1;
	}
	
    ssize_t receive(void* buff, size_t size) {

        size_t probedSize;
		if (!probed.first){
			ssize_t r = probe(probedSize);
			if (r <= 0)	return r;
		} else
			probedSize = probed.second;
		
		if (probedSize > size){
			MTCL_UCX_PRINT(100, "[internal]:\t", "HandleUCX::receive EMSGSIZE, buffer too small\n");
			errno=EMSGSIZE;
			return -1;
		}
		
        ssize_t res = receive_internal(buff, probedSize, true);
        // reset probe size
        probed={false, 0};
        return res;
    }

    ssize_t ireceive(void* buff, size_t size, RequestPool& r) {
        if (probed.first){
			const size_t msg_sz = probed.second;
			auto* req = new requestUCXRecvVar(endpoint, ucp_worker, buff, size, msg_sz,
											  &HandleUCX::rx_done_cb, this);
			if (!req) { errno = ENOMEM; return -1; }
			probed = {false, 0};
			r._getInternalVector<ConnRequestVectorUCX>()->requests.push_back(req);
			enqueue_rx(req);
            return 0;
        }
		requestUCXRecvVar* req = new requestUCXRecvVar(endpoint, ucp_worker, buff, size,
													   &HandleUCX::rx_done_cb, this);       
		if (!req) { errno = ENOMEM; return -1; }
        r._getInternalVector<ConnRequestVectorUCX>()->requests.push_back(req);
		enqueue_rx(req);
        return 0;
    }

    ssize_t ireceive(void* buff, size_t size, Request& r) {
		if (probed.first){
			const size_t msg_sz = probed.second;
			auto* req = new requestUCXRecvVar(endpoint, ucp_worker, buff, size, msg_sz,
											  &HandleUCX::rx_done_cb, this);
			if (!req) { errno = ENOMEM; return -1; }
			probed = {false, 0}; 			
			r.__setInternalR(req);
			enqueue_rx(req);
            return 0;
        }
		requestUCXRecvVar* req = new requestUCXRecvVar(endpoint, ucp_worker, buff, size,
													   &HandleUCX::rx_done_cb, this);
		if (!req) { errno = ENOMEM; return -1; }
		r.__setInternalR(req);
		enqueue_rx(req);
        return 0;
    }


	ssize_t probe(size_t& size, const bool blocking=true) {
		if (probed.first) {
			size = probed.second;
			return (size ? sizeof(size_t) : 0);
		}

		// If we post an independent UCX recv for probing while there are
		// pending framed receives on this stream (i.e., ireceive), the stream can desync.
		// To avoid the problem we advance the current front receive(s) to completion
		// (if blocking), otherwise EWOULDBLOCK.
		if (!rx_queue.empty()) {
			if (!blocking) { errno = EWOULDBLOCK; return -1; }
			// blocking: drive the current front receive(s) to completion
			while (!rx_queue.empty()) {
				int done = 0;
				if (rx_queue.front()->test(done) < 0) break; 
				if (!done) rx_queue.front()->make_progress();
			}
		}
		
		if (pending_probe_req == nullptr) {
			fill_request_param(&pending_probe_ctx, &pending_probe_param, false);
			pending_probe_param.op_attr_mask |= UCP_OP_ATTR_FIELD_FLAGS;
			pending_probe_param.flags = UCP_STREAM_RECV_FLAG_WAITALL;
			pending_probe_param.cb.recv_stream = stream_recv_cb;

			pending_probe_res = 0;
			pending_probe_req = ucp_stream_recv_nbx(endpoint,
													&pending_probe_word,
													sizeof(uint64_t),
													&pending_probe_res,
													&pending_probe_param);

			if (pending_probe_req == nullptr) {  // recv completed
				size = (size_t)be64toh(pending_probe_word);
				probed = {true, size};
				return (size ? sizeof(size_t) : 0);
			}

			if (UCS_PTR_IS_ERR(pending_probe_req)) {
				ucs_status_t st = UCS_PTR_STATUS(pending_probe_req);
				pending_probe_req = nullptr;
				errno = EINVAL;
				return st;
			}
		}
		if (!blocking) {
			ucp_worker_progress(ucp_worker);
			if (pending_probe_ctx.complete == 0) {
				errno = EWOULDBLOCK;
				return -1;
			}
		} else {
			while (pending_probe_ctx.complete == 0) {
				ucp_worker_progress(ucp_worker);
			}
		}

		ucs_status_t st = ucp_request_check_status(pending_probe_req);
		ucp_request_free(pending_probe_req);
		pending_probe_req = nullptr;
		pending_probe_ctx.complete = 0;
		
		if (st == UCS_ERR_CONNECTION_RESET) {
			size = 0;
			probed = {true, 0};
			return 0;
		}
		if (st != UCS_OK) {
			errno = EINVAL;
			return -1;
		}
		
		size = be64toh(pending_probe_word);
		probed = {true, size};
		return (size ? sizeof(size_t) : 0);
	}

    bool has_pending_rx() const { return !rx_queue.empty();	}
	
	bool cancel_pending_probe() {
		if (pending_probe_req == nullptr) return false;
		if (UCS_PTR_IS_ERR(pending_probe_req)) {
			pending_probe_req = nullptr;
			return false;
		}
		ucp_request_cancel(ucp_worker, pending_probe_req);
		return true;
	}
	void cleanup_pending_probe() {
		if (pending_probe_req == nullptr) return;

		if (UCS_PTR_IS_ERR(pending_probe_req)) {
			pending_probe_req = nullptr;
			probed = {false, 0};
			return;
		}

		ucs_status_t st = ucp_request_check_status(pending_probe_req);
		while (st == UCS_INPROGRESS) {
			ucp_worker_progress(ucp_worker);
			st = ucp_request_check_status(pending_probe_req);
		}

		ucp_request_free(pending_probe_req);
		pending_probe_req = nullptr;
		pending_probe_ctx.complete = 0;
		probed = {false, 0};
	}
	
    bool peek() {
        size_t sz;
        ssize_t res = this->probe(sz, false);
        return res > 0;
    }

    ~HandleUCX() { }

};

class ConnUCX : public ConnType {

protected:

    /* OOB-related */
    std::string address;
    int         port;
    fd_set      set, tmpset;
    int         listen_sck;

#if defined(NO_MTCL_MULTITHREADED)
        int fdmax;
#else	
    std::atomic<int> fdmax;
    std::shared_mutex shm;
#endif

    /* UCX-related */
    ucp_address_t*  local_addr;
    size_t          local_addr_len;
    ucp_worker_h    ucp_worker;
    ucp_context_h   ucp_context;

    // UCX endpoint object --> <handle, to_manage>
    std::map<ucp_ep_h, std::pair<HandleUCX*, bool>> connections;

private:

    int _init() {
        if ((listen_sck=socket(AF_INET, SOCK_STREAM, 0)) < 0){
			MTCL_UCX_PRINT(100, "ConnUCX::_init socket errno=%d\n", errno);
            return -1;
        }

        int enable = 1;
        // enable the reuse of the address
        if (setsockopt(listen_sck, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) {
			MTCL_UCX_PRINT(100, "ConnUCX::_init setsockopt errno=%d\n", errno);
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
			MTCL_UCX_PRINT(100, "ConnUCX::_init getaddrinfo errno=%d\n", errno);		
			return -1;
		}

		bool ok = false;
		for (rp = result; rp != NULL; rp = rp->ai_next) {
			if (bind(listen_sck, rp->ai_addr, (int)rp->ai_addrlen) < 0){
				MTCL_UCX_PRINT(100, "ConnUCX::_init bind errno=%d, continue\n", errno);
				continue;
			}
			ok = true;
			break;
		}
		free(result);
		if (!ok) {
			MTCL_UCX_PRINT(100, "ConnUCX::_init bind loop exit with errno=%d\n", errno);
			return -1;
		}
        if (::listen(listen_sck, UCX_BACKLOG) < 0){
			MTCL_UCX_PRINT(100, "ConnUCX::_init listen errno=%d\n", errno);
            return -1;
        }

        return 0;
    }


    int exchange_address(int oob_sock, ucp_address_t* local_addr, size_t local_addr_len, ucp_address_t** peer_addr, size_t* peer_addr_len) {
        int ret;
        uint64_t addr_len = 0;

        ret = send(oob_sock, &local_addr_len, sizeof(local_addr_len), 0);
        if(ret != (int)sizeof(local_addr_len)) {
            MTCL_UCX_ERROR("ConnUCX::exchange_address OOB error sending address len\n");
            return 1;
        }

        ret = send(oob_sock, local_addr, local_addr_len, 0);
        if(ret != (int)local_addr_len) {
            MTCL_UCX_ERROR("ConnUCX::exchange_address OOB error sending address\n");
            return 1;
        }

        ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
        if(ret != (int)sizeof(addr_len)) {
            MTCL_UCX_ERROR("ConnUCX::exchange_address OOB error receiving address len\n");
            return 1;
        }

        *peer_addr_len = addr_len;
        *peer_addr = (ucp_address_t*)malloc(*peer_addr_len);
        if(!*peer_addr) {
            MTCL_UCX_ERROR("ConnUCX::exchange_address OOB error allocating address\n");
            return 1;
        }

        ret = recv(oob_sock, *peer_addr, *peer_addr_len, MSG_WAITALL);
        if(ret != (int)*peer_addr_len) {
            MTCL_UCX_ERROR("ConnUCX::exchange_address OOB error receiving address\n");
            return 1;
        }

        return 0;
    }


    void ep_close(ucp_ep_h ep) {
        // ucp_request_param_t param;
        ucs_status_t status;
        void *close_req;

        // param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
        // param.flags        = UCP_EP_CLOSE_MODE_FLUSH;
        close_req          = ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FLUSH);

        if (close_req == NULL) {
            return;
        } else if (UCS_PTR_IS_ERR(close_req)) {
            MTCL_UCX_ERROR("ConnUCX::ep_close failed with status [%s]\n", ucs_status_string(UCS_PTR_STATUS(close_req)));
            return;
        }

        if (UCS_PTR_IS_PTR(close_req)) {
            do {
                ucp_worker_progress(ucp_worker);
                status = ucp_request_check_status(close_req);
            } while (status == UCS_INPROGRESS);
            ucp_request_free(close_req);
        } else {
            status = UCS_PTR_STATUS(close_req);
        }

        if (status != UCS_OK) {
            MTCL_UCX_PRINT(100, "ConnUCX::ep_close failed to close ep %p. [%s]\n", (void*)ep, ucs_status_string(status));
        }
    }


public:

    int init(std::string) {
        /* OOB init */
        FD_ZERO(&set);
        FD_ZERO(&tmpset);
        listen_sck = -1;
        fdmax = -1;

        /* UCX init */
        ucs_status_t        ep_status = UCS_OK;
        ucp_config_t*       config;
        ucp_params_t        ucp_params;
        ucp_worker_params_t worker_params{};

        memset(&ucp_params, 0, sizeof(ucp_params));
        memset(&worker_params, 0, sizeof(worker_params));


        // Reads environment configuration
        ep_status = ucp_config_read(NULL, NULL, &config);
        if(ep_status != UCS_OK) {
            MTCL_UCX_PRINT(100, "ConnUCX::init error reading config\n");
            errno = EINVAL;
            return -1;
        }

        /* UCX context initialization */
        ucp_params.field_mask   = UCP_PARAM_FIELD_FEATURES;
        ucp_params.features     = UCP_FEATURE_STREAM;

        // Initialize context with requested features and parameters
        ep_status = ucp_init(&ucp_params, config, &ucp_context);
        if(ep_status != UCS_OK) {
            MTCL_UCX_PRINT(100, "ConnUCX::init error initializing context\n");
            errno = EINVAL;
            return -1;
        }
#ifdef UCX_DEBUG
        MTCL_UCX_PRINT(1, "\n\n=== Environment configuration ===\n");
        ucp_config_print(config, stdout, NULL, UCS_CONFIG_PRINT_CONFIG);
        MTCL_UCX_PRINT(1, "\n\n=== Context info ===\n");
        ucp_context_print_info(ucp_context, stderr);
#endif
        ucp_config_release(config);


        /* UCX worker initialization */
        worker_params.field_mask    = UCP_WORKER_PARAM_FIELD_THREAD_MODE;

		// *********************** IMPORTANT *****************************
		// UCS_THREAD_MODE_MULTI has a measurable overhead because UCX must be 
		// internally thread-safe.
		//
		// TODO: Prefer UCS_THREAD_MODE_SINGLE when MTCL guarantees that all UCX
		// operations on a given worker (including ucp_worker_progress) are executed by
		// a single thread. UCS_THREAD_MODE_MULTI must be the default, but SINGLE mode
		// could be forced if SINGLE_IO_THREAD and MTCL_UCX_FORCE_THREAD_SINGLE.
		//
        worker_params.thread_mode   = UCS_THREAD_MODE_MULTI;

        // Initialize worker
        ep_status = ucp_worker_create(ucp_context, &worker_params, &ucp_worker);
        if(ep_status != UCS_OK) {
            MTCL_UCX_PRINT(100, "ConnUCX::init error initializing worker\n");
            errno = EINVAL;
            return -1;
        }

        // Retrieve local address of the current worker, to be exchanged upon
        // handshake with the OOB connections
        ep_status = ucp_worker_get_address(ucp_worker, &local_addr, &local_addr_len);
        if(ep_status != UCS_OK) {
            MTCL_UCX_PRINT(100, "ConnUCX::init error retrieving worker address\n");
            errno = EINVAL;
            return -1;
        }

        return 0;
    }


    int listen(std::string s) {
        address = s.substr(0, s.find(":"));
        port    = stoi(s.substr(address.length()+1));

        if(this->_init()) {
            MTCL_UCX_PRINT(100, "ConnUCX::listen error initializing OOB socket\n");
            errno = EINVAL;
            return -1;
        } 
        
        MTCL_UCX_PRINT(1, "ConnUCX::listen OOB socket listening on: %s:%d\n", address.c_str(), port);

        FD_SET(listen_sck, &set);
        fdmax = listen_sck;

        return 0;
    }


    Handle* connect(const std::string& address, int retry, unsigned timeout_ms) {
		
		int fd=internal_connect(address, retry, timeout_ms);
		if (fd==-1) return nullptr;
		
        // To store address info of the remote peer
        ucp_address_t* peer_addr;
        size_t peer_addr_len;
        int res = exchange_address(fd, local_addr, local_addr_len, &peer_addr, &peer_addr_len);
        if(res != 0) {
            MTCL_UCX_PRINT(100, "ConnUCX::connect error exchanging address, handle is invalid\n");
			close(fd);
            return nullptr;
        }
		
        // Create endpoint with info received from OOB socket
        ucp_ep_h server_ep;
        ucp_ep_params_t ep_params;
        ep_params.field_mask    = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; // |
                                // UCP_EP_PARAM_FIELD_USER_DATA;
        ep_params.address       = peer_addr;
        //ep_params.user_data = &ep_status;

        ucs_status_t ep_status = UCS_OK;
        ep_status = ucp_ep_create(ucp_worker, &ep_params, &server_ep);
        if(ep_status != UCS_OK) {
            MTCL_UCX_PRINT(100, "ConnUCX::connect error creating the endpoint\n");
			close(fd);
            errno = EINVAL;
            return nullptr;
        }
        free(peer_addr);

#ifdef UCX_DEBUG
        MTCL_UCX_PRINT(1, "\n\n=== Endpoint info ===\n");
        ucp_ep_print_info(server_ep, stderr);
#endif

        HandleUCX *handle = new HandleUCX(this, server_ep, ucp_worker);
        {
			REMOVE_CODE_IF(std::unique_lock lock(shm));
			connections.insert({server_ep, {handle, false}});
		}
        MTCL_UCX_PRINT(100, "Connect ok to UCX:%s\n", address.c_str());
        return handle;
    }


    void update() {
        REMOVE_CODE_IF(std::unique_lock ulock(shm, std::defer_lock));

        REMOVE_CODE_IF(ulock.lock());
        tmpset = set;
        REMOVE_CODE_IF(ulock.unlock());

        struct timeval wait_time = {.tv_sec = 0, .tv_usec=UCX_POLL_TIMEOUT};
        int nready = 0;
        
        // Only if we are listening for new connections
        if(fdmax != -1) {
            switch (nready=select(fdmax+1, &tmpset, NULL, NULL, &wait_time)) {
            case -1: {
                if(errno == EBADF) {
                    MTCL_UCX_PRINT(100, "ConnUCX::update select ERROR: errno=EBADF\n");
                    return;
                }
                MTCL_UCX_ERROR("ConnUCX::update select ERROR: errno=%d -- %s\n", errno, strerror(errno));
            }
            case 0:
                break;
            }

            // Only for new connections on the OOB socket
            if(FD_ISSET(this->listen_sck, &tmpset)) {
                int connfd = accept(this->listen_sck, (struct sockaddr*)NULL ,NULL);
                if (connfd == -1){
                    MTCL_UCX_ERROR("ConnUCX::update accept ERROR: errno=%d -- %s\n", errno, strerror(errno));
                    return;
                }

                ucp_address_t* peer_addr;
                size_t peer_addr_len;
                int res = exchange_address(connfd, local_addr, local_addr_len, &peer_addr, &peer_addr_len);
                if(res != 0) {
                    MTCL_UCX_PRINT(100, "ConnUCX::update error exchanging address\n");
                    return;
                }

                // We only use connfd for the OOB handshake
                close(connfd);

                ucs_status_t ep_status   = UCS_OK;
                ucp_ep_h client_ep;
                ucp_ep_params_t ep_params;
                ep_params.field_mask      = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
                                            UCP_EP_PARAM_FIELD_USER_DATA;
                ep_params.address         = peer_addr;
                ep_params.user_data       = &ep_status;

                ep_status = ucp_ep_create(ucp_worker, &ep_params, &client_ep);
                if(ep_status != UCS_OK) {
                    MTCL_UCX_PRINT(100, "ConnUCX::update error creating the endpoint\n");
                    return;
                }
                free(peer_addr);

                HandleUCX* handle = new HandleUCX(this, client_ep, ucp_worker);

                REMOVE_CODE_IF(ulock.lock());
                connections.insert({client_ep, {handle, false}});
                REMOVE_CODE_IF(ulock.unlock());            
                addinQ(true, handle);
            }
        }

        // Poll on managed endpoints to detect something is ready to be read
        // This will cause, at the same time, progress on the user handles
        size_t size = -1;
        size_t max_eps = connections.size();

        // One-shot progress
        int prog = -1;
        prog = ucp_worker_progress(ucp_worker);
        (void)prog;

        // The polling mechanism provided by ucp_stream_worker_poll does not
        // return twice the same endpoint if no NEW network events are detected.
        // This means, with repeated polling if some network event is detected
        // but not managed by the user (by a receive/probe) the endpoint will
        // not be displayed again even if data are ready to be read
        ucp_stream_poll_ep_t* ready_eps = new ucp_stream_poll_ep_t[max_eps];
        size = ucp_stream_worker_poll(ucp_worker, ready_eps, max_eps, 0);
        if(size < 0) {
            MTCL_UCX_PRINT(100, "ConnUCX::update error in ucp_stream_worker_poll\n");
            return;
        }

        REMOVE_CODE_IF(ulock.lock());
        // Some of the endpoints are ready, we need to check if we are managing that ep
        for(size_t i=0; i<size; i++) {
            ucp_stream_poll_ep_t ep = ready_eps[i];

            auto it = connections.find(ep.ep);
            if(it != connections.end()) {
                auto& handlePair = it->second;
                if(handlePair.second) {
                    handlePair.second = false;
                    addinQ(false, handlePair.first);
                }
			}
        }
        REMOVE_CODE_IF(ulock.unlock());

        delete[] ready_eps;
        return;
    }
	
    void notify_yield(Handle* h) {

        HandleUCX* handle = reinterpret_cast<HandleUCX*>(h);
        if(handle->isClosed()) {
			return;
		}

        // UCX stream polling (ucp_stream_worker_poll) reports endpoints
        // only when new network events are detected. If the handle is yielded while
        // there is already pending data (or EOS/RESET) in the stream buffer, the poll
        // may not report the endpoint again. Therefore, before marking the handle as
        // "to_manage", we must probe once in non-blocking mode and enqueue the handle
        // immediately if anything is already observable
        size_t size = 0;
        const ssize_t pr = handle->probe(size, false);
        if (pr >= 0) {
            // Data ready (pr>0) or EOS/RESET already observable (pr==0).
            addinQ(false, handle);
            return;
        }
        if (errno != EWOULDBLOCK && errno != EAGAIN) {
            // Some terminal condition already observable, wake the runtime.
			addinQ(false, handle);
            return;
        }
		// EWOULDBLOCK
		if (handle->has_pending_rx()) {
			addinQ(false, handle);
			return;
		}
        REMOVE_CODE_IF(std::unique_lock l(shm));
        auto it = connections.find(handle->endpoint);
        if(it == connections.end()) {
            MTCL_UCX_ERROR("Couldn't yield handle\n");
			return;
		}
        connections[handle->endpoint].second = true;
		
        return;
    }

    void notify_close(Handle* h, bool close_wr=true, bool close_rd=true) {
        HandleUCX* handle = reinterpret_cast<HandleUCX*>(h);
		
        if(handle->already_closed) return;
		if (!close_rd) return;
		
        REMOVE_CODE_IF(std::unique_lock l(shm));
		connections.erase(handle->endpoint);
		handle->already_closed = true;
		REMOVE_CODE_IF(l.unlock());

		handle->cancel_pending_probe();
		ep_close(handle->endpoint);
		handle->cleanup_pending_probe();
    }

    void end(bool blockflag=false) {
        if(connections.empty()) return;
        auto modified_connections = connections;
        for(auto& [_, handlePair] : modified_connections) {
            if(handlePair.first->already_closed) continue;
			setAsClosed(handlePair.first, blockflag);
        }
        
        ucp_worker_release_address(ucp_worker, local_addr);
        ucp_worker_destroy(ucp_worker);
        ucp_cleanup(ucp_context);
    }

};

} // namespace
