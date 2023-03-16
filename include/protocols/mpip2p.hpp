#ifndef MPIP2P_HPP
#define MPIP2P_HPP

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>

#include <sys/types.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <vector>
#include <queue>
#include <map>
#include <shared_mutex>
#include <thread>
#include <pthread.h>
#include <atomic>
#include <signal.h>

#include <mpi.h>

#include "../handle.hpp"
#include "../protocolInterface.hpp"
#include "../utils.hpp"
#include "../config.hpp"


class HandleMPIP2P : public Handle {

public:
    int rank = -1;
    MPI_Comm server_comm; // MPI communicator for the specific p2p connection
    bool closing = false;
    HandleMPIP2P(ConnType* parent, int rank, MPI_Comm server_comm, bool busy=true) : Handle(parent), rank(rank), server_comm(server_comm) {}

    ssize_t send(const void* buff, size_t size) {
        /*MPI_Request request;
        if (MPI_Isend(buff, size, MPI_BYTE, rank, 0, server_comm, &request) != MPI_SUCCESS) {
			MTCL_MPIP2P_PRINT(100, "HandleMPIP2P::send MPI_Isend ERROR\n");
            errno = ECOMM;	
            return -1;
		}

        int flag = 0;
        MPI_Status status;
        while(!flag && !closing) {
            MPI_Test(&request, &flag, &status);
            checkClosing();
        }

        if(!flag && closing) {
			MTCL_MPIP2P_PRINT(100, "HandleMPIP2P::send MPI_Test ERROR\n");
            errno = ECONNRESET;
            return -1;
        }*/

        if (MPI_Send(&size, 1, MPI_UNSIGNED_LONG, this->rank, 0, this->server_comm) != MPI_SUCCESS){
            MTCL_MPI_PRINT(100, "HandleMPIP2P::send MPI_Send Header ERROR\n");
            errno = ECOMM;
            return -1;
        }
    
        if (MPI_Send(buff, size, MPI_BYTE, this->rank, 0, this->server_comm) != MPI_SUCCESS){
            MTCL_MPI_PRINT(100, "HandleMPIP2P::send MPI_Send Payload ERROR\n");
            errno = ECOMM;
            return -1;
        }

        return size;
    }

    ssize_t receive(void* buff, size_t size){
        /*MPI_Status status; 
        int count, flag;
        while(true) {
            if(MPI_Iprobe(rank, 0, server_comm, &flag, &status)!=MPI_SUCCESS) {
				MTCL_MPIP2P_PRINT(100, "HandleMPIP2P::receive MPI_Iprobe ERROR\n");
				errno = ECOMM;
                return -1;
            }
            if(flag) {
                if (MPI_Recv(buff, size, MPI_BYTE, rank, MPI_ANY_TAG, server_comm, &status) != MPI_SUCCESS) {
					MTCL_MPIP2P_PRINT(100, "HandleMPIP2P::receive: MPI_Recv ERROR\n");
					errno = ECOMM;
					return -1;
				}
                MPI_Get_count(&status, MPI_BYTE, &count);
                return count;
            }
            else if(closing) {
                // MPI_Comm_free(&server_comm);
                return 0;
            }
			checkClosing();

            if constexpr (MPIP2P_POLL_TIMEOUT)
				std::this_thread::sleep_for(std::chrono::microseconds(MPIP2P_POLL_TIMEOUT));
        }        
        return -1;*/

        int r;
        MPI_Status s;
        if (MPI_Recv(buff, size, MPI_BYTE, this->rank, 0, this->server_comm, &s) != MPI_SUCCESS){
            MTCL_MPI_PRINT(100, "HandleMPIP2P::receive MPI_Recv ERROR\n");
			errno = ECOMM;
			return -1;
        }
        MPI_Get_count(&s, MPI_BYTE, &r);
        return r;
    }

    ssize_t probe(size_t& size, const bool blocking=true){
        int f;
        if (!blocking){
            if (MPI_Iprobe(this->rank, 0, this->server_comm, &f, MPI_STATUS_IGNORE) != MPI_SUCCESS){
                MTCL_MPI_PRINT(100, "HandleMPIP2P::probe MPI_Iprobe ERROR\n");
                errno = ECOMM;
                return -1;
            } 
            if (!f){
                errno = EWOULDBLOCK;
                return -1;
            } 
        }
        
        if (f || blocking){
            if (MPI_Recv(&size, 1, MPI_UNSIGNED_LONG, this->rank, 0, this->server_comm, MPI_STATUS_IGNORE) != MPI_SUCCESS){
                MTCL_MPI_PRINT(100, "HandleMPIP2P::probe MPI_Recv ERROR\n");
                errno = ECOMM;
                return -1;
            }
        }
        return sizeof(size_t);
    }

    bool peek() {
        int f = 0;
        if (MPI_Iprobe(this->rank, 0, MPI_COMM_WORLD, &f, MPI_STATUS_IGNORE) != MPI_SUCCESS){
            MTCL_MPI_PRINT(100, "HandleMPI::peek MPI_Iprobe ERROR\n");
            errno = ECOMM;
            return -1;
        }

        return f;
    }

    ssize_t sendEOS() {
		size_t sz = 0;
		return MPI_Send(&sz, 1, MPI_UNSIGNED_LONG, this->rank, 0, this->server_comm); 
	}

    ~HandleMPIP2P() {}
};


class ConnMPIP2P : public ConnType {
protected:
    char portname[MPI_MAX_PORT_NAME];
    std::string published_label;
    
    int rank;
    std::atomic<bool> finalized = false;
    bool listening = false;
    
    std::map<HandleMPIP2P*, bool> connections;  // Active connections for this Connector
    std::map<MPI_Comm, int> comm_sizes;
    std::shared_mutex shm;

    inline static std::thread t1;
    pthread_t pt1;
    MPI_Errhandler errhdl;

public:

   ConnMPIP2P(){};
   ~ConnMPIP2P(){};

    int init(std::string) {
        int provided;
        if (MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided) != MPI_SUCCESS) {
			MTCL_MPIP2P_PRINT(100, "ConnMPIP2P::init: MPI_Init_thread error\n");
			errno = EINVAL;
            return -1;
		}

        // no thread support 
        if (provided < MPI_THREAD_MULTIPLE){
			MTCL_MPIP2P_PRINT(100, "ConnMPIP2P::init: no thread support in MPI\n");
			errno = EINVAL;
			return -1;
        }

        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        return 0;
    }

    void listen_threadF(char* portname, const char* published_label) {

        listening = true;
        try {
            MPI_Publish_name(published_label, MPI_INFO_NULL, portname);
        }
        catch(...) {
            MTCL_MPIP2P_PRINT(100, "ConnMPIP2P::listen: MPI_Publish_name ERROR\n");
			return;
        }
        
        while(!finalized) {
            MPI_Comm client;
            if (MPI_Comm_accept(portname, MPI_INFO_NULL, 0, MPI_COMM_SELF, &client) != MPI_SUCCESS) {
				MTCL_MPIP2P_ERROR("listen_threadF: MPI_Comm_accept error\n");
				continue;
			}

            int remote_size = 0;
            MPI_Comm_remote_size(client, &remote_size);
            MTCL_MPIP2P_PRINT(100, "remote size is %d\n", remote_size);

            /* Retrieve the remote size and create one handle for each of the
             * connecting rank */
            REMOVE_CODE_IF(std::unique_lock ulock(shm));
            comm_sizes.insert({client, remote_size});
            for(int i=0; i<remote_size; i++) {

                HandleMPIP2P* handle = new HandleMPIP2P(this, i, client, false);
                {
                    connections.insert({handle, false});
                    ADD_CODE_IF(addinQ(true, handle));  
                }
                REMOVE_CODE_IF(addinQ(true, handle));
            }
        }
		MTCL_MPIP2P_PRINT(100, "Accept thread finalized.\n");        
    }

    int listen(std::string s) {
        published_label = s;
        MPI_Open_port(MPI_INFO_NULL, portname);
        MTCL_MPIP2P_PRINT(1, "listening on portname: %s - with label: %s\n", portname, s.c_str());

        t1 = std::thread([&](){listen_threadF(portname, published_label.c_str());});

        return 0;
    }

    void update() {
        int flag;
        MPI_Status status;

        REMOVE_CODE_IF(std::unique_lock ulock(shm));
        for (auto& [handle, to_manage] : connections) {
            if(to_manage) {

                if (MPI_Iprobe(handle->rank, 0, handle->server_comm, &flag, &status) != MPI_SUCCESS) {
					MTCL_MPIP2P_ERROR("ConnMPIP2P::update: MPI_Iprobe ERROR (CONNECTION)\n");
					errno = ECOMM;
					throw;
				}
                if(flag) {
                    to_manage = false;
					// NOTE: called with shm lock hold. Double lock if there is the IO-thread!
                    addinQ(false, handle);  
                }
            }
        }		
    }


    Handle* connect(const std::string& address) {
        try {
            MPI_Lookup_name(address.c_str(), MPI_INFO_NULL, portname);
        }
        catch(...) {
            MTCL_MPIP2P_PRINT(100, "ConnMPIP2P::connect: MPI_Lookup_name ERROR\n");
            errno = EINVAL;
            return nullptr;
        }
        MPI_Comm server_comm;
        if (MPI_Comm_connect(portname, MPI_INFO_NULL, 0, MPI_COMM_WORLD, &server_comm) != MPI_SUCCESS) {
			MTCL_MPIP2P_PRINT(100, "ConnMPIP2P::connect: MPI_Comm_connect ERROR\n");
			errno = ECOMM;
			return nullptr;
		}

        HandleMPIP2P* handle = new HandleMPIP2P(this, 0, server_comm, true);
		{
			std::unique_lock lock(shm);
			connections[handle] = false;
		}
        MTCL_MPIP2P_PRINT(100, "ConnMPIP2P::connect Connected to: %s\n", portname);        
        return handle;
    }


    void notify_close(Handle* h, bool close_wr=true, bool close_rd=true) {        
        HandleMPIP2P* handle = reinterpret_cast<HandleMPIP2P*>(h);

        if(connections.count(handle) == 0) return;

        REMOVE_CODE_IF(std::unique_lock l(shm));
        if (close_rd) {
            connections.erase(handle);
            int size = -42;
            auto it = comm_sizes.find(handle->server_comm);
            if(it != comm_sizes.end())
                size = --it->second;
            if(size == 0) {
                comm_sizes.erase(handle->server_comm);
                MPI_Comm_disconnect(&handle->server_comm);
            }
        }       

    }


    void notify_yield(Handle* h) override {
        HandleMPIP2P* handle = reinterpret_cast<HandleMPIP2P*>(h);
        {
            REMOVE_CODE_IF(std::unique_lock l(shm));
            auto it = connections.find(handle);
            if (it != connections.end())
                it->second = true;
        }

        /*
        if (handle->closing) {
            addinQ(false, h);
            return;
        }
        REMOVE_CODE_IF(std::unique_lock l(shm));
        connections[handle] = true;
        */
    }

    void end(bool blockflag=false) {
        auto modified_connections = connections;
        for(auto& [handle, to_manage] : modified_connections)
            if(to_manage)
                setAsClosed(handle, true);

        finalized = true;
        if(listening) {
            MPI_Comm c;
            char* a[2];
            a[0] = portname;
            a[1] = NULL;

            /* Spawns utility MPI process to issue a fake MPI_Comm_connect and
             * wake up the blocking accept
             */
            if (MPI_Comm_spawn(MPIP2P_STOP_PROCESS, a, 1, MPI_INFO_NULL, 0, MPI_COMM_SELF, &c, MPI_ERRCODES_IGNORE) != MPI_SUCCESS) {
				MTCL_MPIP2P_ERROR("ConnMPIP2P::end(): MPI_Comm_spawn ERROR\n");
			}
            t1.join();

            MPI_Unpublish_name(published_label.c_str(), MPI_INFO_NULL, portname);
            MPI_Close_port(portname);
        }

        MPI_Finalize();
    }

};

#endif
