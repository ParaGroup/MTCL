#ifndef HANDLE_HPP
#define HANDLE_HPP

#include <iostream>
#include <atomic>

#include "protocolInterface.hpp"
#include "utils.hpp"

enum HandleType {
    BROADCAST,
    FANIN,
    FANOUT,
    GATHER,
    P2P,
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
     * @brief Send \b size byte of \b buff to the remote end connected to this
     * Handle. Wait until all data has been sent or until the peer close the
     * connection.
     * 
     * @param buff data to be sent
     * @param size amount of bytes to send
     * @return number of bytes sent to the remote end or \c -1 if an error occurred.
     * If \c -1 is returned, the error can be checked via \b errno.
     */
    virtual ssize_t send(const void* buff, size_t size) = 0; 


    /**
     * @brief Check for incoming message and write in \b size the amount of data
     * present in the message.
     * 
     * @param[out] size total size in byte of incoming message
     * @param[in] blocking if true, the probe call blocks until a message
     * is ready to be received. If false, the call returns immediately and sets
     * \b errno to \b EWOULDBLOCK if no message is present on this handle.
     * @return ssize_t \c sizeof(size_t) upon success. If \c -1 is returned,
     * the error can be checked via \b errno.
     */
	virtual ssize_t probe(size_t& size, const bool blocking=true)=0;
	

    /**
     * @brief Read at most \b size byte into \b buff from the remote end connected
     * to this Handle. Wait until all \b size data has been received or until the
     * connection is closed by the remote peer.
     * 
     * @param buff 
     * @param size 
     * @return the amount of bytes read upon success, \c 0 in case the connection
     * has been closed, \c -1 if an error occurred. If \c -1 is returned, the error
     * can be checked via \b errno.
     */
    virtual ssize_t receive(void* buff, size_t size) = 0;

    virtual void yield() = 0;
    virtual void close(bool close_wr=true, bool close_rd=true) = 0;


    virtual ssize_t sendrecv(const void* sendbuff, size_t sendsize, void* recvbuff, size_t recvsize) {
        MTCL_PRINT(100, "[internal]:\t", "CommunicationHandle::sendrecv invalid operation.\n");
        errno = EINVAL;
        return -1;
    }

    virtual int getSize() {return 1;}
    void setName(const std::string &name) { handleName = name; }
	const std::string& getName() { return handleName; }
    HandleType getType() {return type;}


	const bool isClosed()   { return closed_rd && closed_wr; }
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
						MTCL_PRINT(100, "[internal]:\t", "ConnType::setAsClosed probe error\n");
						return;
					}
				}
				if(sz == 0) break;
				MTCL_ERROR("[internal]:\t", "Spurious message received of size %ld on handle with name %s!\n", sz, h->getName().c_str());
				char* buff = new char[sz];
				if(h->receive(buff, sz) == -1) {
					MTCL_PRINT(100, "[internal]:\t", "ConnType::setAsClosed receive error\n");
					return;
				}
				h->probed={false,0};
				delete[] buff;
			}
		}
	}

    // Finally closing the handle
    h->close(false, true);

}

#endif
