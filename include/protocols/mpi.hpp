#pragma once

#include <vector>
#include <map>
#include <tuple>
#include <shared_mutex>
#include <thread>
#include <errno.h>
#include <atomic>

#include <mpi.h>

#include "../handle.hpp"
#include "../protocolInterface.hpp"
#include "../utils.hpp"
#include "../config.hpp"
#include "../async.hpp"

namespace MTCL {

class HandleMPI; // forward declaration for requestMPI class

class ConnRequestVectorMPI : public ConnRequestVector {
    MPI_Request* requestArray;
    size_t arraySize = 0;
    unsigned int currSize = 0;
public:
    ConnRequestVectorMPI(size_t sizeHint = 1) : arraySize(sizeHint){
        requestArray = (MPI_Request*)calloc(sizeHint, sizeof(MPI_Request));
    }

    ~ConnRequestVectorMPI(){
        free(requestArray);
    }

    bool testAll(){
        int flag;
        MPI_Testall(currSize, requestArray, &flag, MPI_STATUSES_IGNORE);
        return flag;
    }

    void waitAll(){
        MPI_Waitall(currSize, requestArray, MPI_STATUSES_IGNORE);
    }

    void reset(){
        currSize = 0;
    }

    inline void grow(){
        requestArray = (MPI_Request*)realloc(requestArray, 2*arraySize*sizeof(MPI_Request));
        arraySize *= 2;
    }

    inline MPI_Request* getNextRequest(){
        if (currSize == arraySize) grow();
        return &requestArray[currSize++];
    }

    void pushRequest(MPI_Request&& r){
         if (currSize == arraySize) grow();
        requestArray[currSize++] = std::move(r);
    }
};

class requestMPI : public request_internal {
    friend class HandleMPI;
    MPI_Request requests = MPI_REQUEST_NULL;
    size_t size;
	MPI_Status status{};
	ssize_t got = -1;
	int last_mpi_rc = MPI_SUCCESS;
	
    int test(int& result) {
        int rc = MPI_Test(&requests, &result, &status);
        last_mpi_rc = rc;
        if (rc != MPI_SUCCESS) {
            result = 0;
            if (status.MPI_ERROR == MPI_ERR_TRUNCATE) errno = EMSGSIZE;
            else errno = ECOMM;
			MTCL_MPI_PRINT(100, "requestMPI::test MPI_Test ERROR\n");
			return -1;
		}
        if (result && got < 0) {
            int c = MPI_UNDEFINED;
            MPI_Get_count(&status, MPI_BYTE, &c);
            if (c != MPI_UNDEFINED) got = c;
            else got = (ssize_t)size;
        }
        return 0;
    }

    int make_progress(){
        if constexpr (MPI_MAKE_PROGRESS_TIME > 0)
            std::this_thread::sleep_for(std::chrono::microseconds(MPI_MAKE_PROGRESS_TIME));
        return 0;
    }

    int wait(){
		int rc = MPI_Wait(&requests, &status);
        last_mpi_rc = rc;
        if (rc != MPI_SUCCESS) {
            if (status.MPI_ERROR == MPI_ERR_TRUNCATE) errno = EMSGSIZE;
			else errno = ECOMM;
			MTCL_MPI_PRINT(100, "requestMPI::wait MPI_Wait ERROR\n");
            if (got < 0) {
                int c = MPI_UNDEFINED;
                MPI_Get_count(&status, MPI_BYTE, &c);
                if (c != MPI_UNDEFINED) got = c;
            }
			return -1;
		}
        if (got < 0) {
            int c = MPI_UNDEFINED;
            MPI_Get_count(&status, MPI_BYTE, &c);
            if (c != MPI_UNDEFINED) got = c;
            else got = (ssize_t)size;
        }
		return 0;
	}

    ssize_t count() const override {
        if (got >= 0) return got;
        return (ssize_t)size;
    }
};

class HandleMPI : public Handle {
	
public:
    bool closing = false;
    int rank;
    int tag;
    HandleMPI(ConnType* parent, int rank, int tag): Handle(parent), rank(rank), tag(tag){}
	
    ssize_t isend(const void* buff, size_t size, Request& r) {
        requestMPI* requestPtr = new requestMPI;

		requestPtr->size = size;
        if (MPI_Isend(buff, size, MPI_BYTE, rank, tag, MPI_COMM_WORLD, &requestPtr->requests) != MPI_SUCCESS){
	        MTCL_MPI_PRINT(100, "HandleMPI::send MPI_Isend ERROR\n");
            errno = ECOMM;
            return -1;
        }

        r.__setInternalR(requestPtr);
	    return 0;
    }

    ssize_t isend(const void* buff, size_t size, RequestPool& r) {
        MPI_Request* req = r._getInternalVector<ConnRequestVectorMPI>()->getNextRequest();
        
        if (MPI_Isend(buff, size, MPI_BYTE, rank, tag, MPI_COMM_WORLD, req) != MPI_SUCCESS){
	        MTCL_MPI_PRINT(100, "HandleMPI::send MPI_Isend ERROR\n");
            errno = ECOMM;
            return -1;
        }
	    return 0;
    }

    ssize_t send(const void* buff, size_t size) {
        
        if (MPI_Send(buff, size, MPI_BYTE, this->rank, this->tag, MPI_COMM_WORLD) != MPI_SUCCESS){
            MTCL_MPI_PRINT(100, "HandleMPI::send MPI_Send Payload ERROR\n");
            errno = ECOMM;
            return -1;
        }
        return size;
    }


    ssize_t receive(void* buff, size_t size){
        int r=0;
        MPI_Status s;

		int rc = MPI_Recv(buff, (int)size, MPI_BYTE, this->rank, this->tag, MPI_COMM_WORLD, &s);
		if (rc != MPI_SUCCESS) {
            MTCL_MPI_PRINT(100, "HandleMPI::receive MPI_Recv ERROR\n");
			if (s.MPI_ERROR == MPI_ERR_TRUNCATE) errno = EMSGSIZE;
			else errno = ECOMM;
			return -1;
        }
        MPI_Get_count(&s, MPI_BYTE, &r);
		if (r == 0) { // EOS
			this->closed_rd = true;
		}
        return r;
    }

    ssize_t ireceive(void* buff, size_t size, RequestPool& r){
        MPI_Request* req = r._getInternalVector<ConnRequestVectorMPI>()->getNextRequest();

        if (MPI_Irecv(buff, size, MPI_BYTE, this->rank, this->tag, MPI_COMM_WORLD, req) != MPI_SUCCESS){
            MTCL_MPI_PRINT(100, "HandleMPI::receive MPI_Recv ERROR\n");
			errno = ECOMM;
			return -1;
        }
        return 0;
    }

    ssize_t ireceive(void* buff, size_t size, Request& r){
        requestMPI* requestPtr = new requestMPI;
		requestPtr->size = size;
        if (MPI_Irecv(buff, size, MPI_BYTE, this->rank, this->tag, MPI_COMM_WORLD, &requestPtr->requests) != MPI_SUCCESS){
            MTCL_MPI_PRINT(100, "HandleMPI::receive MPI_Recv ERROR\n");
			errno = ECOMM;
			return -1;
        }
        r.__setInternalR(requestPtr);
        return 0;
    }

    ssize_t probe(size_t& size, const bool blocking=true){
        int f = 0, c;
        MPI_Status s;
        if (!blocking){
            if (MPI_Iprobe(this->rank, this->tag, MPI_COMM_WORLD,&f, &s) != MPI_SUCCESS){
                MTCL_MPI_PRINT(100, "HandleMPI::probe MPI_Iprobe ERROR\n");
                errno = ECOMM;
                return -1;
            } 
            if (!f){
                errno = EWOULDBLOCK;
                return -1;
            }
        } else 
            if (MPI_Probe(this->rank, this->tag, MPI_COMM_WORLD, &s) != MPI_SUCCESS){
                MTCL_MPI_PRINT(100, "HandleMPI::probe MPI_Probe ERROR\n");
                errno = ECOMM;
                return -1;
            }

        MPI_Get_count(&s, MPI_BYTE, &c);
        size = c;
        if (!c) {
			MPI_Recv(nullptr, 0, MPI_CHAR, this->rank, this->tag,
						 MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			this->closed_rd = true;
		}
        
		return (size ? (ssize_t)sizeof(size_t): 0);
    }

    bool peek() {
        int f = 0;
        if (MPI_Iprobe(this->rank, this->tag, MPI_COMM_WORLD, &f, MPI_STATUS_IGNORE) != MPI_SUCCESS){
            MTCL_MPI_PRINT(100, "HandleMPI::peek MPI_Iprobe ERROR\n");
            errno = ECOMM;
            return -1;
        }

        return f;
    }

    ssize_t sendEOS() {
		return MPI_Send(nullptr, 0, MPI_BYTE, this->rank, this->tag, MPI_COMM_WORLD); 
	}
};



class ConnMPI : public ConnType {
protected:
    int rank;

    // <rank, tag> => <HandleMPI, busy>
    std::map<std::pair<int, int>, std::pair<HandleMPI*, bool>> connections;
    std::shared_mutex shm;
    std::atomic<unsigned int> tag_counter_even = 100, tag_counter_odd = 101;

public:

    int init(std::string) {
        int provided;
        if (MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided) != MPI_SUCCESS) {
			MTCL_MPI_PRINT(100, "ConnMPI::init: MPI_Init_thread ERROR\n");
			errno = EINVAL;
            return -1;
		}
		
        // no thread support 
        if (provided < MPI_THREAD_MULTIPLE){
			MTCL_MPI_PRINT(100, "ConnMPI::init: no thread support in MPI\n");
			errno= EINVAL;
            return -1;
        }

        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN); // force return on error
		return 0;
    }

    int listen(std::string s) {
		MTCL_MPI_PRINT(1, "listening on: %s\n", s.c_str());
        return 0;
    }


    Handle* connect(const std::string& dest, int, unsigned) {
    
        int rank;
        try {
            rank = stoi(dest.substr(0, dest.find(":")));
        }
        catch(std::invalid_argument&) {
            MTCL_MPI_PRINT(100, "ConnMPI::connect rank must be an integer greater or equal than 0\n");
            errno = EINVAL;
            return nullptr;
        }
        
        int tag;

        if(rank < 0) {
			MTCL_MPI_PRINT(100, "ConnMPI::connect the connection rank must be greater or equal than 0\n");
            errno = EINVAL;
            return nullptr;
        }


        if (this->rank < rank)
            tag = tag_counter_even.fetch_add(2); 
        else
            tag = tag_counter_odd.fetch_add(2);

        int header[1];
        header[0] = tag;
        if (MPI_Send(header, 1, MPI_INT, rank, MPI_CONNECTION_TAG, MPI_COMM_WORLD) != MPI_SUCCESS) {
			MTCL_MPI_PRINT(100, "ConnMPI::connect MPI_Send ERROR\n");
			errno = ECOMM;
			return nullptr;
		}

        HandleMPI* handle = new HandleMPI(this, rank, tag);
		{
			REMOVE_CODE_IF(std::unique_lock lock(shm));
			connections.insert({{rank, tag}, {handle, false}});
		}

        MTCL_MPI_PRINT(100, "Connection ok to MPI:%s\n", dest.c_str());

        return handle;
    }

    void update() {

        REMOVE_CODE_IF(std::unique_lock ulock(shm, std::defer_lock));

        int flag;
        MPI_Status status;
        if (MPI_Iprobe(MPI_ANY_SOURCE, MPI_CONNECTION_TAG, MPI_COMM_WORLD, &flag, &status) != MPI_SUCCESS) {
			MTCL_MPI_ERROR("ConnMPI::update: MPI_Iprobe ERROR (CONNECTION)\n");
			errno = ECOMM;
			return;
		}
        if(flag) {
            int headersLen;
            MPI_Get_count(&status, MPI_INT, &headersLen);
            int header[headersLen];
            
            if (MPI_Recv(header, headersLen, MPI_INT, status.MPI_SOURCE, MPI_CONNECTION_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
				MTCL_MPI_ERROR("ConnMPI::update: MPI_Recv ERROR (CONNECTION)\n");
				errno = ECOMM;
                return;
            }
            
            int source = status.MPI_SOURCE;
            int source_tag = header[0];
            HandleMPI* handle = new HandleMPI(this, source, source_tag);
            REMOVE_CODE_IF(ulock.lock());			
            connections.insert({{source, source_tag},{handle, false}});
            REMOVE_CODE_IF(ulock.unlock());
            addinQ(true, handle);
        }
		
        REMOVE_CODE_IF(ulock.lock());
        for (auto& [rankTagPair, handlePair] : connections) {
            if(handlePair.second) {
                if (MPI_Iprobe(rankTagPair.first, rankTagPair.second, MPI_COMM_WORLD, &flag, &status) != MPI_SUCCESS) {
					MTCL_MPI_ERROR("ConnMPI::update: MPI_Iprobe ERROR\n");
					errno = ECOMM;
					return;
				}
                if (flag) {
                    handlePair.second = false;
					// NOTE: called with ulock lock hold. Double lock if there is the IO-thread!
                    addinQ(false, handlePair.first);
                }
            }
        }
        REMOVE_CODE_IF(ulock.unlock());        
    }


    void notify_close(Handle* h, bool close_wr=true, bool close_rd=true) {
        HandleMPI* hMPI = reinterpret_cast<HandleMPI*>(h);
                    
        REMOVE_CODE_IF(std::unique_lock l(shm));

        if (close_rd)
            connections.erase({hMPI->rank, hMPI->tag});
	}
	

    void notify_yield(Handle* h) {
        HandleMPI* hMPI = reinterpret_cast<HandleMPI*>(h);
        
        {
            REMOVE_CODE_IF(std::unique_lock l(shm));
            auto it = connections.find({hMPI->rank, hMPI->tag});
            if (it != connections.end())
                it->second.second = true;
        }
	}

    void end(bool blockflag=false) {
		// Snapshot and clear the connection table under lock.
		decltype(connections) local;
		{
			REMOVE_CODE_IF(std::unique_lock l(shm));
			local.swap(connections);
		}

		for (auto& [_, handlePair] : local) {
			setAsClosed(handlePair.first, blockflag);
		}
		
		// Best-effort drain of unexpected/pending messages (including connections)
		// to prevent MPI_Finalize from hanging on some MPI implementations.
		while (true) {
			int flag = 0;
			MPI_Status st;
			int rc = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &st);
			if (rc != MPI_SUCCESS || !flag) break;
			
			if (st.MPI_TAG == MPI_CONNECTION_TAG) {
				int nints = 0;
				MPI_Get_count(&st, MPI_INT, &nints);
				if (nints < 0) nints = 0;
				if (nints > 0) {
					std::vector<int> tmp((size_t)nints);
					MPI_Recv(tmp.data(), nints, MPI_INT, st.MPI_SOURCE, st.MPI_TAG,
							 MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				} else {
					MPI_Recv(nullptr, 0, MPI_INT, st.MPI_SOURCE, st.MPI_TAG,
							 MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				}
			} else {
				int nbytes = 0;
				MPI_Get_count(&st, MPI_BYTE, &nbytes);
				if (nbytes < 0) nbytes = 0;
				if (nbytes > 0) {
					std::vector<char> tmp((size_t)nbytes);
					MPI_Recv(tmp.data(), nbytes, MPI_BYTE, st.MPI_SOURCE, st.MPI_TAG,
							 MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				} else {
					MPI_Recv(nullptr, 0, MPI_BYTE, st.MPI_SOURCE, st.MPI_TAG,
							 MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				}
			}
		}
		
		MPI_Finalize();
	}
};

} // namespace

