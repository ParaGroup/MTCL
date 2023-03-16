#ifndef COLLECTIVECONTEXT_HPP
#define COLLECTIVECONTEXT_HPP

#include <iostream>
#include <map>
#include <vector>
#include "../utils.hpp"
#include "collectiveImpl.hpp"
#include "../handle.hpp"

#ifdef ENABLE_MPI
#include "mpiImpl.hpp"
#endif

#ifdef ENABLE_UCX
#include "uccImpl.hpp"
#endif


class CollectiveContext : public CommunicationHandle {
    friend class Manager;

protected:
    int size;
    bool root;
    int rank;
    CollectiveImpl* coll;
    bool canSend, canReceive;
    bool completed = false;


    void incrementReferenceCounter() {counter++;}
    void decrementReferenceCounter() {
        counter--;
        if (counter == 0 && closed_wr && closed_rd){
            delete this;
        }
    }

public:
    CollectiveContext(int size, bool root, int rank, HandleType type,
            bool canSend=false, bool canReceive=false) : size(size), root(root),
                rank(rank), canSend(canSend), canReceive(canReceive) {
                    this->type = type;
                    closed_rd = !canReceive;
                    closed_wr = !canSend;
                    counter = 1;
    }

    bool setImplementation(ImplementationType impl, std::vector<Handle*> participants, int uniqtag) {
        const std::map<HandleType, std::function<CollectiveImpl*()>> contexts = {
            {BROADCAST,  [&]{
                    CollectiveImpl* coll = nullptr;
                    switch (impl) {
                        case GENERIC:
                            coll = new BroadcastGeneric(participants, root, uniqtag);
                            break;
                        case MPI:
                            #ifdef ENABLE_MPI
                            coll = new BroadcastMPI(participants, root, uniqtag);
                            #endif
                            break;
                        case UCC:
                            #ifdef ENABLE_UCX
                            coll = new BroadcastUCC(participants, rank, size, root, uniqtag);
                            #endif
                            break;
                        default:
                            coll = nullptr;
                            break;
                    }
                    return coll;
                }
            },
            {FANIN,  [&]{return new FanInGeneric(participants, root, uniqtag);}},
            {FANOUT, [&]{return new FanOutGeneric(participants, root, uniqtag);}},
            {GATHER,  [&]{
                    CollectiveImpl* coll = nullptr;
                    switch (impl) {
                        case GENERIC:
                            coll = new GatherGeneric(participants, root, rank, uniqtag);
                            break;
                        case MPI:
                            #ifdef ENABLE_MPI
                            coll = new GatherMPI(participants, root, uniqtag);
                            #endif
                            break;
                        case UCC:
                            #ifdef ENABLE_UCX
                            coll = new GatherUCC(participants, rank, size, root, uniqtag);
                            #endif
                            break;
                        default:
                            coll = nullptr;
                            break;
                    }
                    return coll;
                }}

        };

        if (auto found = contexts.find(type); found != contexts.end()) {
            coll = found->second();
            if(!coll) 
                MTCL_ERROR("[internal]: \t", "CollectiveContext::setImplementation implementation type not enabled\n");

        } else {
            MTCL_ERROR("[internal]: \t", "CollectiveContext::setImplementation implementation type not found\n");
            coll = nullptr;
        }

        // true if coll != nullptr
        return coll;
    }

    /**
     * @brief Updates the status of the collective during the creation and
     * checks if the team is ready to be used.
     * 
     * @param[in] count number of received connections
     * @return true if the collective group is ready, false otherwise
     */
    bool update(int count) {
        completed = count == (size - 1); 

        return completed;
    }

    /**
     * @brief Receives at most \b size data into \b buff based on the
     * semantics of the collective.
     * 
     * @param[out] buff buffer used to write data
     * @param[in] size maximum amount of data to be written in the buffer
     * @return ssize_t if successful, returns the amount of data written in the
     * buffer. Otherwise, -1 is return and \b errno is set.
     */
    ssize_t receive(void* buff, size_t size) {
        if(!canReceive) {
            MTCL_PRINT(100, "[internal]:\t", "CollectiveContext::receive invalid operation for the collective\n");
            errno = EINVAL;
            return -1;
        }

        return coll->receive(buff, size);
    }
    
    /**
     * @brief Sends \b size bytes of \b buff, following the semantics of the collective.
     * 
     * @param[in] buff buffer of data to be sent
     * @param[in] size amount of data to be sent
     * @return ssize_t if successful, returns \b size. Otherwise, -1 is returned
     * and \b errno is set.
     */
    ssize_t send(const void* buff, size_t size) {
        if(!canSend) {
            MTCL_PRINT(100, "[internal]:\t", "CollectiveContext::send invalid operation for the collective\n");
            errno = EINVAL;
            return -1;
        }

        return coll->send(buff, size);
    }


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
	ssize_t probe(size_t& size, const bool blocking=true) {
        if(!canReceive) {
            MTCL_PRINT(100, "[internal]:\t", "CollectiveContext::probe invalid operation for the collective\n");
            errno = EINVAL;
            return -1;
        }

        return coll->probe(size, blocking);
    }

    bool peek() {
        return coll->peek();
    }

    ssize_t sendrecv(const void* sendbuff, size_t sendsize, void* recvbuff, size_t recvsize) {
        return coll->sendrecv(sendbuff, sendsize, recvbuff, recvsize);
    }

    void close(bool close_wr=true, bool close_rd=true) {
        closed_rd = closed_rd || close_rd;
        coll->close(close_wr && !closed_wr, close_rd);
        closed_wr = closed_wr || close_wr;
    }

    int getSize() {
        return size;
    }

    void finalize(bool blockflag, std::string name="") {
        coll->finalize(blockflag, name);
    }

    void yield();

    virtual ~CollectiveContext() {delete coll;};
};


CollectiveContext *createContext(HandleType type, int size, bool root, int rank)
{
    const std::map<HandleType, std::function<CollectiveContext*()>> contexts = {
        {BROADCAST,  [&]{return new CollectiveContext(size, root, rank, type, false, false);}},
        {FANIN,  [&]{return new CollectiveContext(size, root, rank, type, !root, root);}},
        {FANOUT,  [&]{return new CollectiveContext(size, root, rank, type, root, !root);}},
        {GATHER,  [&]{return new CollectiveContext(size, root, rank, type, false, false);}}

    };

    if (auto found = contexts.find(type); found != contexts.end()) {
        return found->second();
    } else {
        return nullptr;
    }
}

#endif //COLLECTIVECONTEXT_HPP
