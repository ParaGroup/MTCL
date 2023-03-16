#ifndef MPI_HPP
#define MPI_HPP

#include <vector>
#include <map>
#include <tuple>
#include <shared_mutex>
#include <thread>
#include <errno.h>

#include <mpi.h>

#include "../handle.hpp"
#include "../protocolInterface.hpp"
#include "../utils.hpp"
#include "../config.hpp"



class HandleMPI : public Handle {
	
public:
    bool closing = false;
    int rank;
    int tag;
    HandleMPI(ConnType* parent, int rank, int tag): Handle(parent), rank(rank), tag(tag){}
	
    /*ssize_t send(const void* buff, size_t size) {
        MPI_Request request;
        if (MPI_Isend(buff, size, MPI_BYTE, rank, tag, MPI_COMM_WORLD, &request) != MPI_SUCCESS){
			MTCL_MPI_PRINT(100, "HandleMPI::send MPI_Isend ERROR\n");
            errno = ECOMM;
            return -1;
        }
            
        int flag = 0;
        MPI_Status status;
        while(!flag && !closing) {
            MPI_Test(&request, &flag, &status);
        }

        if(!flag && closing) {
			MTCL_MPI_PRINT(100, "HandleMPI::send MPI_Test ERROR\n");
            errno = ECONNRESET;
            return -1;
        }

        return size;
    }*/

    ssize_t send(const void* buff, size_t size) {
        if (MPI_Send(&size, 1, MPI_UNSIGNED_LONG, this->rank, this->tag, MPI_COMM_WORLD) != MPI_SUCCESS){
            MTCL_MPI_PRINT(100, "HandleMPI::send MPI_Send Header ERROR\n");
            errno = ECOMM;
            return -1;
        }
    
        if (MPI_Send(buff, size, MPI_BYTE, this->rank, this->tag, MPI_COMM_WORLD) != MPI_SUCCESS){
            MTCL_MPI_PRINT(100, "HandleMPI::send MPI_Send Payload ERROR\n");
            errno = ECOMM;
            return -1;
        }
        return size;
    }

    /*ssize_t receive(void* buff, size_t size){
        MPI_Status status; 
        int count;
        int flag = 0;
		if constexpr (MPI_POLL_TIMEOUT) {
			while(true){
				if (MPI_Iprobe(rank, tag, MPI_COMM_WORLD, &flag, &status) != MPI_SUCCESS) {
					MTCL_MPI_PRINT(100, "HandleMPI::receive MPI_Iproble ERROR\n");
					errno = ECOMM;
					return -1;
				}				
				if (flag) {
					if (MPI_Recv(buff, size, MPI_BYTE, rank, tag, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
						MTCL_MPI_PRINT(100, "HandleMPI::receive MPI_Recv ERROR\n");
						errno = ECOMM;
						return -1;
					}
					MPI_Get_count(&status, MPI_BYTE, &count);
					return count;
				} else if (closing) return 0;
				std::this_thread::sleep_for(std::chrono::microseconds(MPI_POLL_TIMEOUT));
			}
        }
		if (MPI_Recv(buff, size, MPI_BYTE, rank, tag, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
			MTCL_MPI_PRINT(100, "HandleMPI::receive MPI_Recv ERROR\n");
			errno = ECOMM;
			return -1;
		}
		MPI_Get_count(&status, MPI_BYTE, &count);
		return count;
    }*/

    ssize_t receive(void* buff, size_t size){
        int r;
        MPI_Status s;
        if (MPI_Recv(buff, size, MPI_BYTE, this->rank, this->tag, MPI_COMM_WORLD, &s) != MPI_SUCCESS){
            MTCL_MPI_PRINT(100, "HandleMPI::receive MPI_Recv ERROR\n");
			errno = ECOMM;
			return -1;
        }
        MPI_Get_count(&s, MPI_BYTE, &r);
        return r;
    }

    ssize_t probe(size_t& size, const bool blocking=true){
        int f = 0;
        if (!blocking){
            if (MPI_Iprobe(this->rank, this->tag, MPI_COMM_WORLD,&f, MPI_STATUS_IGNORE) != MPI_SUCCESS){
                MTCL_MPI_PRINT(100, "HandleMPI::probe MPI_Iprobe ERROR\n");
                errno = ECOMM;
                return -1;
            } 
            if (!f){
                errno = EWOULDBLOCK;
                return -1;
            } 
        }
        
        if (f || blocking){
            if (MPI_Recv(&size, 1, MPI_UNSIGNED_LONG, this->rank, this->tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE) != MPI_SUCCESS){
                MTCL_MPI_PRINT(100, "HandleMPI::probe MPI_Recv ERROR\n");
                errno = ECOMM;
                return -1;
            }
        }
        return sizeof(size_t);
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
		size_t sz = 0;
		return MPI_Send(&sz, 1, MPI_UNSIGNED_LONG, this->rank, this->tag, MPI_COMM_WORLD); 
	}
};



class ConnMPI : public ConnType {
protected:
    int rank;

    // <rank, tag> => <HandleMPI, busy>
    std::map<std::pair<int, int>, std::pair<HandleMPI*, bool>> connections;
    std::shared_mutex shm;

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
		return 0;
    }

    int listen(std::string s) {
		MTCL_MPI_PRINT(1, "listening on: %s\n", s.c_str());
        return 0;
    }


    Handle* connect(const std::string& dest, int, unsigned) {
        // in pratica questo specifica il tag utilizzato per le comunicazioni successive
        // per ora solo tag, poi si vede

        //parse della stringa
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
        try {
            tag = stoi(dest.substr(dest.find(":") + 1, dest.length()));
        }
        catch(std::invalid_argument&) {
            MTCL_MPI_PRINT(100, "ConnMPI::connect rank must be an integer greater than 0\n");
            errno = EINVAL;
            return nullptr;
        }

        if(rank < 0) {
			MTCL_MPI_PRINT(100, "ConnMPI::connect the connection rank must be greater or equal than 0\n");
            errno = EINVAL;
            return nullptr;
        }

        if (tag <= (int)MPI_CONNECTION_TAG){
			MTCL_MPI_PRINT(100, "ConnMPI::connect the connection tag must be greater than 0\n");
			errno = EINVAL;
            return nullptr;
        }
        
        int header[1];
        header[0] = tag;
        if (MPI_Send(header, 1, MPI_INT, rank, MPI_CONNECTION_TAG, MPI_COMM_WORLD) != MPI_SUCCESS) {
			MTCL_MPI_PRINT(100, "ConnMPI::connect MPI_Send ERROR\n");
			errno = ECOMM;
			return nullptr;
		}

        // creo l'handle
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
			throw;
		}
        if(flag) {
            int headersLen;
            MPI_Get_count(&status, MPI_INT, &headersLen);
            int header[headersLen];
            
            if (MPI_Recv(header, headersLen, MPI_INT, status.MPI_SOURCE, MPI_CONNECTION_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
				MTCL_MPI_ERROR("ConnMPI::update: MPI_Recv ERROR (CONNECTION)\n");
				errno = ECOMM;
                throw;
            }
            
            int source = status.MPI_SOURCE;
            int source_tag = header[0];
            HandleMPI* handle = new HandleMPI(this, source, source_tag);
            REMOVE_CODE_IF(ulock.lock());			
            connections.insert({{source, source_tag},{handle, false}});
            addinQ(true, handle);
            REMOVE_CODE_IF(ulock.unlock());
        }
		
        REMOVE_CODE_IF(ulock.lock());
        for (auto& [rankTagPair, handlePair] : connections) {
            if(handlePair.second) {
                if (MPI_Iprobe(rankTagPair.first, rankTagPair.second, MPI_COMM_WORLD, &flag, &status) != MPI_SUCCESS) {
					MTCL_MPI_ERROR("ConnMPI::update: MPI_Iprobe ERROR\n");
					errno = ECOMM;
					throw;
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
        auto modified_connections = connections;
        for(auto& [_, handlePair] : modified_connections)
            setAsClosed(handlePair.first, true);
        MPI_Finalize();
    }
};



#endif
