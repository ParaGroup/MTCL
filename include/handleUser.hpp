#ifndef HANDLEUSER_HPP
#define HANDLEUSER_HPP

#ifndef MTCL_DISABLE_COLLECTIVES
#include "collectives/collectiveContext.hpp"
#endif
#include "handle.hpp"
#include "errno.h"

class HandleUser {
    friend class ConnType;
    friend class Manager;
    CommunicationHandle* realHandle;
    bool isReadable    = false;
    bool newConnection = true;
public:
    HandleUser() : HandleUser(nullptr, false, false) {}
    HandleUser(CommunicationHandle* h, bool r, bool n): realHandle(h),
            isReadable(r), newConnection(n) {
		if (h) h->incrementReferenceCounter();
    }

    HandleUser(const HandleUser&) = delete;
    HandleUser& operator=(HandleUser const&) = delete;
    HandleUser& operator=(HandleUser && o) {
		if (this != &o) {
			realHandle    = o.realHandle;
			isReadable    = o.isReadable;
			newConnection = o.newConnection;
			o.realHandle  = nullptr;
			o.isReadable  = false;
			o.newConnection=false;
		}
		return *this;
	}
	
    HandleUser(HandleUser&& h) :
		realHandle(h.realHandle), isReadable(h.isReadable), newConnection(h.newConnection) {
        h.realHandle = nullptr;
		h.isReadable = h.newConnection = false;
    }
    
    // releases the handle to the manager
    void yield() {
        isReadable = false;
        newConnection = false;
        if (realHandle) realHandle->yield();
    }

    bool isValid() {
        return realHandle;
    }

    bool isNewConnection() {
        return newConnection;
    }

    size_t getID(){
        return (size_t)realHandle;
    }

	const std::string& getName() { return realHandle->getName(); }
	void setName(const std::string& name) { realHandle->setName(name);}
	
    ssize_t send(const void* buff, size_t size){
		/*if (!isWritable) {
			MTCL_PRINT(100, "[internal]:\t", "HandleUser::send EBADF (1)\n");
            errno = EBADF; // the "communicator" is not valid or closed
			return -1;
		}*/
        newConnection = false;
        if (!realHandle || realHandle->closed_wr) {
			MTCL_PRINT(100, "[internal]:\t", "HandleUser::send EBADF (2)\n");
            errno = EBADF; // the "communicator" is not valid or closed
            return -1;
        }
        return realHandle->send(buff, size);
    }

	ssize_t probe(size_t& size, const bool blocking=true) {
        newConnection = false;
		if (realHandle->probed.first) { // previously probed, return 0 if EOS received
			size=realHandle->probed.second;
			return (size?sizeof(size_t):0);
		}
        if (!isReadable){
			MTCL_PRINT(100, "[internal]:\t", "HandleUser::probe handle not readable\n");
			return 0;
        }
        if (!realHandle) {
			MTCL_PRINT(100, "[internal]:\t", "HandleUser::probe EBADF\n");
            errno = EBADF; // the "communicator" is not valid or closed
            return -1;
        }
		if (realHandle->closed_rd) return 0;

		// reading the header to get the size of the message
		ssize_t r;
		if ((r=realHandle->probe(size, blocking))<=0) {
			switch(r) {
			case 0: {
				isReadable=false;
				realHandle->close(true, true);
				return 0;
			}
			case -1: {	
                if (errno==EINVAL) {
                    return -1;
                }			
				if (errno==ECONNRESET) {
					realHandle->close(true, true);
					return 0;
				}
				if (errno==EWOULDBLOCK || errno==EAGAIN) {
					errno = EWOULDBLOCK;
					return -1;
				}
			}}
			return r;
		}
		realHandle->probed={true,size};
		if (size==0) { // EOS received
			realHandle->close(false, true);
			isReadable=false;
			return 0;
		}
		return r;		
	}

    ssize_t receive(void* buff, size_t size) {
		size_t sz;
		if (!realHandle->probed.first) {
			// reading the header to get the size of the message
			ssize_t r;
			if ((r=this->probe(sz, true))<=0) {
				return r;
			}
		} else {
			newConnection = false;
			if (!isReadable){
				MTCL_PRINT(100, "[internal]:\t", "HandleUser::probe handle not readable\n");
				return 0;
			}
			if (!realHandle) {
				MTCL_PRINT(100, "[internal]:\t", "HandleUser::probe EBADF\n");
				errno = EBADF; // the "communicator" is not valid or closed
				return -1;
			}
			if (realHandle->closed_rd) return 0;
		}
		if ((sz=realHandle->probed.second)>size) {
			MTCL_ERROR("[internal]:\t", "HandleUser::receive ENOMEM, receiving less data\n");
			errno=ENOMEM;
			return -1;
		}	   
		realHandle->probed={false,0};
		return realHandle->receive(buff, std::min(sz,size));
    }

    ssize_t sendrecv(const void* sendbuff, size_t sendsize, void* recvbuff, size_t recvsize) {
		realHandle->probed={false,0};
        return realHandle->sendrecv(sendbuff, sendsize, recvbuff, recvsize);
    }

    void close(){
        if (realHandle) realHandle->close(true, false);
    }

    int size() {
        return realHandle->getSize();
    }

	std::pair<bool, bool> isClosed(){
		if (!realHandle) return {true, true};
		return {realHandle->closed_rd, realHandle->closed_wr};
	}

    HandleType getType() {
        if(realHandle)
            return realHandle->getType();
        else
            return INVALID_TYPE;
    }

    ~HandleUser(){
        // if this handle is readable and it is not closed, when i destruct this handle implicitly i'm giving the control to the runtime.
        if (isReadable && realHandle) this->yield();

        // decrement the reference counter of the wrapped handle to manage its destruction.
        if (realHandle) {
			realHandle->decrementReferenceCounter();
		}
    }


};

#endif

