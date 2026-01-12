#pragma once

#include <iostream>
#include <atomic>

#include "protocolInterface.hpp"
#include "utils.hpp"
#include "async.hpp"

namespace MTCL {

enum HandleType {
    MTCL_BROADCAST,
    MTCL_SCATTER,
    MTCL_FANIN,
    MTCL_FANOUT,
    MTCL_GATHER,
    MTCL_ALLGATHER,
    MTCL_ALLTOALL,
    P2P,
    PROXY,
    INVALID_TYPE
};

class CommunicationHandle {
    friend class HandleUser;
	friend class FanInGeneric;
	friend class FanOutGeneric;
    friend class Manager;

protected:
	std::string handleName{"no-name-provided"};
    std::pair<bool, size_t> probed{false,0};  
	std::atomic<bool> closed_rd = false, closed_wr = false;
    std::atomic<int> counter = 0;
    HandleType type = P2P;


    virtual void incrementReferenceCounter() = 0;
    virtual void decrementReferenceCounter() = 0;

public:

	/**
	 * @brief Send a message of \b size bytes from \b buff to the remote peer.
	 *
	 * This is a blocking operation: it returns only after the full message has been
	 * sent according to the backend transport semantics, or after an error occurs.
	 *
	 * @param buff Source buffer containing the data to send.
	 * @param size Number of bytes to send.
	 *
	 * @return On success, returns the number of bytes sent (typically \b size).
	 *         Returns \c -1 on error and sets \b errno accordingly.
	 */
	virtual ssize_t send(const void* buff, size_t size) = 0;
	
	/**
	 * @brief Post a non-blocking send of one message of \b size bytes from \b buff.
	 *
	 * The call returns after the send operation has been successfully posted to the
	 * underlying backend. The user must keep \b buff valid and unchanged until the
	 * associated request completes.
	 *
	 * Completion semantics:
	 * - The request becomes "complete" when the backend has finished the send
	 *   operation (as defined by the backend semantics).
	 * - After completion, the user may safely reuse or free \b buff.
	 *
	 * @param buff Source buffer (must remain valid until completion).
	 * @param size Number of bytes to send.
	 * @param r    Request (or request pool) used to track completion.
	 *
	 * @return Returns 0 on successful posting, or \c -1 on immediate error with \b errno set.
	 */
	virtual ssize_t isend(const void* buff, size_t size, Request& r) = 0;
	virtual ssize_t isend(const void* buff, size_t size, RequestPool& r) = 0;

	/**
	 * @brief Check whether an incoming message is available and obtain its size.
	 *
	 * If a message is available, this call sets \b size to the message length in bytes.
	 * Depending on the backend, the size information may be cached so that a subsequent
	 * \c receive() can proceed without re-probing.
	 *
	 * @param[out] size Message size in bytes when a message is available.
	 * @param[in]  blocking If true, block until a message is available or the peer closes.
	 *                      If false, return immediately and set \b errno to \c EWOULDBLOCK
	 *                      (or \c EAGAIN) if no message is currently available.
	 *
	 * @return Returns a positive value on success (implementation-defined, typically
	 *         \c sizeof(size_t)). Returns \c 0 if the peer has closed the connection (EOS).
	 *         Returns \c -1 on error and sets \b errno accordingly.
	 */
	virtual ssize_t probe(size_t& size, const bool blocking=true) = 0;

	/**
	 * @brief Receive one message from the remote end into \b buff.
	 *
	 * The parameter \b size is the capacity of the user-provided buffer \b buff.
	 * The implementation may internally read (or have already read via \c probe()) the
	 * message size, then copies at most \b size bytes into \b buff.
	 *
	 * @param buff Destination buffer.
	 * @param size Capacity of \b buff in bytes.
	 *
	 * @return On success, returns the number of bytes actually received and copied
	 *         into \b buff (0 <= n <= size). Returns \c 0 only to signal end-of-stream
	 *         (the peer closed the connection / EOS). Returns \c -1 on error and sets
	 *         \b errno. If the incoming message is larger than \b size, returns \c -1
	 *         and sets \b errno to \c EMSGSIZE (the message is not consumed, and thus 
	 *         may be retried).
	 */
    virtual ssize_t receive(void* buff, size_t size) = 0;

	/**
	 * @brief Post a non-blocking receive of one message into \b buff.
	 *
	 * The parameter \b size is the capacity of the user-provided buffer \b buff.
	 * The operation completes when a full message has been received (and copied)
	 * or when the peer closes the connection (EOS).
	 *
	 * Completion semantics:
	 * - The associated request becomes "complete" when the receive finishes.
	 * - The actual number of bytes received can be retrieved via Request::count()
	 *   after completion.
	 *
	 * @param buff Destination buffer.
	 * @param size Capacity of \b buff in bytes.
	 * @param r    Request object (or pool) used to track completion.
	 *
	 * @return Returns 0 on successful posting of the operation, or \c -1 on immediate
	 *         error (with \b errno set). If the incoming message is larger than \b size,
	 *         the request completes with error \c EMSGSIZE (the message is drained).
	 */
	virtual ssize_t ireceive(void* buff, size_t size, RequestPool& r) = 0;
    virtual ssize_t ireceive(void* buff, size_t size, Request& r) = 0;

	/**
	 * @brief Yield the handle back to the runtime.
	 *
	 * This function informs the runtime that the handle is no longer held for user-side
	 * progress, so the runtime may resume managing receive-side progress for this handle.
	 */
	virtual void yield() = 0;

	/**
	 * @brief Close the handle for writing and/or reading.
	 *
	 * \b close_wr disables further sends. \b close_rd disables further receives.
	 * After closing, the runtime may release resources associated with this handle.
	 *
	 * @param close_wr If true, close the write side (no further sends).
	 * @param close_rd If true, close the read side (no further receives).
	 */
	virtual void close(bool close_wr=true, bool close_rd=true) = 0;
	
	/**
	 * @brief Combined send and receive (optional operation).
	 *
	 * Default implementation returns \c -1 and sets \b errno to \c EINVAL.
	 * Backends may override this to provide an efficient send-receive primitive.
	 *
	 * @param sendbuff Data to send.
	 * @param sendsize Number of bytes to send.
	 * @param recvbuff Destination buffer for received data.
	 * @param recvsize Capacity of \b recvbuff in bytes.
	 * @param datasize Optional element size hint for specific backends.
	 *
	 * @return On success, returns the number of bytes received. Returns \c 0 on EOS.
	 *         Returns \c -1 on error and sets \b errno accordingly.
	 */
	virtual ssize_t sendrecv(const void* sendbuff, size_t sendsize,
							 void* recvbuff, size_t recvsize, size_t datasize = 1) {
		MTCL_PRINT(100, "[MTCL]:", "CommunicationHandle::sendrecv invalid operation.\n");
		errno = EINVAL;
		return -1;
	}

	/**
	 * @brief Return the team size associated with this handle, if applicable.
	 *
	 * For point-to-point handles this returns 1. Collective handles may override 
	 * this to return the team size.
	 */
	virtual int getSize() { return 1; }

	/**
	 * @brief Return the calling process rank within the team, if applicable.
	 *
	 * Returns -1 when the concept of team rank does not apply to this handle.
	 */
	virtual int getTeamRank() { return -1; }

	/**
	 * @brief Return the partition size for collective operations, if applicable.
	 *
	 * Returns -1 when partitioning is not supported or not applicable.
	 */
	virtual int getTeamPartitionSize(size_t buffcount) { return -1; }

	/**
	 * @brief Assign a human-readable name to this handle (for debugging/logging).
	 */
	void setName(const std::string &name) { handleName = name; }

	/**
	 * @brief Get the human-readable name of this handle.
	 */
	const std::string& getName() { return handleName; }
	
	/**
	 * @brief Return the backend type associated with this handle.
	 */
	HandleType getType() { return type; }
	
	/**
	 * @brief Return true if both read and write sides have been closed.
	 */
	const bool isClosed() { return closed_rd && closed_wr; }

};


class Handle : public CommunicationHandle {
    friend class CollectiveImpl;
    // friend class HandleUser;
    friend class Manager;
    friend class ConnType;

    ConnType* parent;
	
    void incrementReferenceCounter(){
        counter++;
    }

    void decrementReferenceCounter(){
        counter--;
        if (counter == 0 && closed_wr && closed_rd){
            delete this;
        }
    }
protected:	
	// if first=true second is the size contained in the header
    virtual ssize_t sendEOS() = 0;


public:
    Handle() {}

    /**
     * @brief Checks if there is something ready to be read on this handle.
     * 
     * @return \b true, if ready, \b false otherwise.
     */
    virtual bool peek() = 0;

    void yield() {
        if (!closed_rd)
            parent->notify_yield(this);
    }

    void close(bool close_wr=true, bool close_rd=true){
		if (close_wr && !closed_wr){
            this->sendEOS();
            closed_wr = true;
        }

        if (close_rd && !closed_rd){
            closed_rd = true;
        }
        
        parent->notify_close(this, closed_wr, closed_rd);

        if (counter == 0 && closed_rd && closed_wr) {
            delete this;
        }
        
        /*if (!closed) {
			parent->notify_close(this, close_wr, close_rd);
			if (close_wr && close_rd) closed=true;
		}
        if (counter == 0 && closed) {
            delete this;
		}*/
    }
    
    Handle(ConnType* parent) : parent(parent) {}	
    virtual ~Handle() {};
};


void ConnType::setAsClosed(Handle* h, bool blockflag){
    // Send EOS if not already sent
    if(!h->closed_wr) {
        h->close(true, false);
	}
	
	if (blockflag) {
		// If EOS is still not received we wait for it, discarding pending messages
		if(!h->closed_rd) {
			size_t sz = 1;
			while(true) {
				if (h->probed.first) {
					sz = h->probed.second;
				} else {
					if(h->probe(sz) == -1) {
						MTCL_PRINT(100, "[MTCL]:", "ConnType::setAsClosed probe error\n");
						return;
					}
				}
				if(sz == 0) break;
				MTCL_PRINT(100, "[MTCL]:", "Spurious message received of size %ld on handle with name %s!\n", sz, h->getName().c_str());
				std::vector<char> buff(sz);
				if(h->receive(buff.data(), sz) == -1) {
					MTCL_PRINT(100, "[MTCL]:", "ConnType::setAsClosed receive error\n");
					return;
				}
			}
		}
	}

    // Finally closing the handle
    h->close(false, true);

}

} // namespace

