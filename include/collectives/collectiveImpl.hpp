#ifndef COLLECTIVEIMPL_HPP
#define COLLECTIVEIMPL_HPP

#include <iostream>
#include <map>
#include <vector>

#include "../handle.hpp"
#include "../utils.hpp"


enum ImplementationType {
    GENERIC,
    MPI,
    UCC
};


/**
 * @brief Interface for transport-specific network functionalities for collective
 * operations. Subclasses specify different behaviors depending on the specific
 * transport used to implement the collective operations and on the specific type
 * of collective.
 * 
 */
class CollectiveImpl {
protected:
    std::vector<Handle*> participants;
	int uniqtag=-1;
	
    //TODO: 
    // virtual bool canSend() = 0;
    // virtual bool canReceive() = 0;

protected:
    ssize_t probeHandle(Handle* realHandle, size_t& size, const bool blocking=true) {
		if (realHandle->probed.first) { // previously probed, return 0 if EOS received
			size=realHandle->probed.second;
			return (size ? sizeof(size_t) : 0);
		}
        if (!realHandle) {
			MTCL_PRINT(100, "[internal]:\t", "CollectiveImpl::probeHandle EBADF\n");
            errno = EBADF; // the "communicator" is not valid or closed
            return -1;
        }
		if (realHandle->closed_rd) return 0;

		// reading the header to get the size of the message
		ssize_t r;
		if ((r=realHandle->probe(size, blocking))<=0) {
			switch(r) {
			case 0: {
				realHandle->close(true, true);
				return 0;
			}
			case -1: {				
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
			return 0;
		}
		return r;		
	}

    ssize_t receiveFromHandle(Handle* realHandle, void* buff, size_t size) {
		size_t sz;
		if (!realHandle->probed.first) {
			// reading the header to get the size of the message
			ssize_t r;
			if ((r=probeHandle(realHandle, sz, true))<=0) {
				return r;
			}
		} else {
			if (!realHandle) {
				MTCL_PRINT(100, "[internal]:\t", "CollectiveImpl::receiveFromHandle EBADF\n");
				errno = EBADF; // the "communicator" is not valid or closed
				return -1;
			}
			if (realHandle->closed_rd) return 0;
		}
		if ((sz=realHandle->probed.second)>size) {
			MTCL_ERROR("[internal]:\t", "CollectiveImpl::receiveFromHandle ENOMEM, receiving less data\n");
			errno=ENOMEM;
			return -1;
		}	   
		realHandle->probed={false,0};
		return realHandle->receive(buff, std::min(sz,size));
    }


public:
    CollectiveImpl(std::vector<Handle*> participants, int uniqtag) : participants(participants),uniqtag(uniqtag) {
        // for(auto& h : participants) h->incrementReferenceCounter();
    }

    /**
     * @brief Checks if any of the participants has something ready to be read.
     * 
     * @return true, if at least one participant has something to read. False
     * otherwise 
     */
    virtual bool peek() {
        //NOTE: the basic implementation returns true as soon as one of the
        //      participants has something to receive. Some protocols may require
        //      to override this method in order to correctly "peek" for messages 
        for(auto& h : participants) {
            if(h->peek()) return true;
        }

        return false;
    }

    virtual ssize_t probe(size_t& size, const bool blocking=true) = 0;
    virtual ssize_t send(const void* buff, size_t size) = 0;
    virtual ssize_t receive(void* buff, size_t size) = 0;
    virtual void close(bool close_wr=true, bool close_rd=true) = 0;

    virtual ssize_t sendrecv(const void* sendbuff, size_t sendsize, void* recvbuff, size_t recvsize) {
        MTCL_PRINT(100, "[internal]:\t", "CollectiveImpl::sendrecv invalid operation for the collective\n");
        errno = EINVAL;
        return -1;
    }

    virtual void finalize(bool, std::string name="") {return;}

    virtual ~CollectiveImpl() {}
};

/**
 * @brief Generic implementation of Broadcast collective using low-level handles.
 * This implementation is intended to be used by those transports that do not have
 * an optimized implementation of the Broadcast collective. This implementation
 * can be selected using the \b BROADCAST type and the \b GENERIC implementation,
 * provided, respectively, by @see CollectiveType and @see ImplementationType. 
 * 
 */
class BroadcastGeneric : public CollectiveImpl {
protected:
    bool root;
    
public:
    ssize_t probe(size_t& size, const bool blocking=true) {
		MTCL_ERROR("[internal]:\t", "Broadcast::probe operation not supported\n");
		errno=EINVAL;
        return -1;
    }

    ssize_t send(const void* buff, size_t size) {
        for(auto& h : participants) {
            if(h->send(buff, size) < 0) {
                errno = ECONNRESET;
                return -1;
            }
        }

        return size;
    }

    ssize_t receive(void* buff, size_t size) {
        auto h = participants.at(0);
        ssize_t res = receiveFromHandle(h, (char*)buff, size);
        if(res == 0) h->close(true, false);

        return res;
    }

    ssize_t sendrecv(const void* sendbuff, size_t sendsize, void* recvbuff, size_t recvsize) {
        if(root) {
            return this->send(sendbuff, sendsize);
        }
        else {
            return this->receive(recvbuff, recvsize);
        }
    }

    void close(bool close_wr=true, bool close_rd=true) {
        // Root process can issue an explicit close to all its non-root processes.
        if(root) {
            for(auto& h : participants) h->close(true, false);
            return;
        }
    }

public:
    BroadcastGeneric(std::vector<Handle*> participants, bool root, int uniqtag) : CollectiveImpl(participants, uniqtag), root(root) {}

};


class FanInGeneric : public CollectiveImpl {
private:
    ssize_t probed_idx = -1;
    bool root;

public:
	// OK
    ssize_t probe(size_t& size, const bool blocking=true) {
        
        ssize_t res = -1;
        auto iter = participants.begin();
        while(res == -1 && !participants.empty()) {
            auto h = *iter;
            res = h->probe(size, false);
            // The handle sent EOS, we remove it from participants and go on
            // looking for a "real" message
            if(res > 0 && size == 0) {
                iter = participants.erase(iter);
                res = -1;
                h->close(true, true);
                if(iter == participants.end()) {
                    if(blocking) {
                        iter = participants.begin();
                        continue;
                    }
                    else break;
                }
            }
            if(res > 0) {
                probed_idx = iter - participants.begin();
				h->probed={true, size};
            }
            iter++;
            if(iter == participants.end()) {
                if(blocking)
                    iter = participants.begin();
                else break;
            }
        }

        // All participants have closed their connection, we "notify" the HandleUser
        // that an EOS has been received for the entire group
        if(participants.empty()) {
            size = 0;
            res = sizeof(size_t);
        }

        return res;
    }

    ssize_t send(const void* buff, size_t size) {
        ssize_t r;
        for(auto& h : participants) {
            if((r = h->send(buff, size)) < 0) return r;
        }

        return size;
    }

    ssize_t receive(void* buff, size_t size) {
        // I already probed one of the handles, I must receive from the same one
        ssize_t r;
        auto h = participants.at(probed_idx);

        if((r = h->receive(buff, size)) <= 0)
            return -1;
        h->probed = {false,0};

        probed_idx = -1;

        return r;
    }

    void close(bool close_wr=true, bool close_rd=true) {
        // Non-root process can send EOS to root and go on.
        if(!root) {
            auto h = participants.at(0);
            h->close(true, false);
            return;
        }	
    }

public:
    FanInGeneric(std::vector<Handle*> participants, bool root, int uniqtag) : CollectiveImpl(participants,uniqtag), root(root) {}

};


class FanOutGeneric : public CollectiveImpl {
private:
    size_t current = 0;
    bool root;

public:
    ssize_t probe(size_t& size, const bool blocking=true) {
        if(participants.empty()) {
            errno = ECONNRESET;
            return -1;
        }

        auto h = participants.at(0);
        ssize_t res = h->probe(size, blocking);
        // EOS
        if(res > 0 && size == 0) {
            participants.pop_back();
            h->close(true, true);
        }
		if (res > 0) 
			h->probed={true, size};
        return res;
    }

    ssize_t send(const void* buff, size_t size) {
        size_t count = participants.size();
        auto h = participants.at(current);
        
        int res = h->send(buff, size);

        ++current %= count;

        return res;
    }

    ssize_t receive(void* buff, size_t size) {
        auto h = participants.at(0);
        ssize_t res = h->receive(buff, size);
        h->probed = {false, 0};

        return res;
    }

    void close(bool close_wr=true, bool close_rd=true) {		
        // Root process can issue the close to all its non-root processes.
        if(root) {
            for(auto& h : participants) h->close(true, false);
            return;
        }
    }

public:
    FanOutGeneric(std::vector<Handle*> participants, bool root, int uniqtag) : CollectiveImpl(participants, uniqtag), root(root) {}

};


class GatherGeneric : public CollectiveImpl {
private:
    size_t current = 0;
    bool root;
    int rank;
    bool allReady{true};
public:
    GatherGeneric(std::vector<Handle*> participants, bool root, int rank, int uniqtag) :
        CollectiveImpl(participants, uniqtag), root(root), rank(rank) {}

    ssize_t probe(size_t& size, const bool blocking=true) {
		MTCL_ERROR("[internal]:\t", "Gather::probe operation not supported\n");
		errno=EINVAL;
        return -1;
    }

    // The buffer must be (participants.size()+1)*size
    ssize_t receive(void* buff, size_t size) {
        for(auto& h : participants) {
            ssize_t res;

            // Receive rank
            int remote_rank;
            res = receiveFromHandle(h, &remote_rank, sizeof(int));
            if(res <= 0) return res;

            // Receive data
            res = receiveFromHandle(h, (char*)buff+(remote_rank*size), size);
            if(res <= 0) return res;
        }

        return sizeof(size_t);
    }

    ssize_t send(const void* buff, size_t size) {
        // Qui c'è solo l'handle del root
        for(auto& h : participants) {
            h->send(&rank, sizeof(int));
            h->send(buff, size);
        }
        
        return sizeof(size_t);
    }

    ssize_t sendrecv(const void* sendbuff, size_t sendsize, void* recvbuff, size_t recvsize) {
        if(root) {
            memcpy((char*)recvbuff+(rank*sendsize), sendbuff, sendsize);
            return this->receive(recvbuff, recvsize);
        }
        else {
            return this->send(sendbuff, sendsize);
        }
    }

    void close(bool close_wr=true, bool close_rd=true) {        
        for(auto& h : participants) {
            h->close(true, false);
        }

        return;
    }
    
    ~GatherGeneric () {}
};

#endif //COLLECTIVEIMPL_HPP
