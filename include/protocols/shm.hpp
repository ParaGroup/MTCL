#ifndef SHM_HPP
#define SHM_HPP

#include <cassert>
#include <cstring>

#include "../handle.hpp"
#include "../protocolInterface.hpp"
#include "shm_buffer.hpp"

namespace MTCL {

class HandleSHM : public Handle {
public:	
	shmBuffer in;
	shmBuffer out;

    HandleSHM(ConnType* parent, shmBuffer& in, shmBuffer& out): Handle(parent), in(in), out(out) {}

	ssize_t sendEOS() {
		return out.put(nullptr, 0);
	}
	
    ssize_t send(const void* buff, size_t size) {
		return out.put(buff,size);
    }

	ssize_t isend(const void* buff, size_t size, Request& r) {
		return out.put(buff,size);
	}
	// receives the header containing the size (sizeof(size_t) bytes)
	ssize_t probe(size_t& size, const bool blocking=true) {
		if (probed.first){
			size = probed.second;
			return (size ? (ssize_t)sizeof(size_t) : 0);
		}
		ssize_t sz;
		if (blocking) {
			sz = in.getsize();
		} else {
			sz = in.trygetsize();
			if (sz<0 && errno == EAGAIN) {
				errno = EWOULDBLOCK;
				return -1;
			}
		}
		if (sz<0) return -1;
		size = (size_t)sz;
		probed = {true, size};

		// If EOS, consume it immediately so that subsequent probes do not see it.
		if (size == 0) {
			char dummy;
			(void)in.tryget(&dummy, 1);
		}
		return (size ? (ssize_t)sizeof(size_t) : 0);
	}
	
    ssize_t receive(void* buff, size_t size) {
		size_t probedSize;
		if (!probed.first){
			ssize_t r = probe(probedSize, true);
			if (r <= 0) return r;
		} else {
			probedSize = probed.second;
		}
		if (probedSize == 0) {
			probed = {false, 0};
			return 0;
		}
		if (probedSize > size){
			errno = ENOMEM;
			return -1;
		}
		probed = {false, 0};
		return in.get(buff, probedSize);
    }

    bool peek() {
		ssize_t r = in.peek();
		return (r > 0);
	}

    ~HandleSHM() {}
};


class ConnSHM : public ConnType {
protected:
	std::string shmname;
	std::atomic<int> shmconnid{0};  // to generate unique name
	
    shmBuffer connbuff;    
    std::map<HandleSHM*, bool> connections;  // Active connections for this Connector

#if defined(NO_MTCL_MULTITHREADED)
    std::shared_mutex shm;
#endif

public:

   ConnSHM(){};
   ~ConnSHM(){};

    int init(std::string name) {
		shmname = name;
		return 0;
	}
	
    int listen(std::string address) {

		//FIX: controllo che l'indirizzo parte con '/' e che non sia piu' lungo di NAME_MAX
		// vale la pena prependere name all'address e mettere noi lo slash?

		if (connbuff.create(address, false)==-1) {
			// If a previous run crashed, the name might still exist.
			if (errno == EEXIST) {
				if (connbuff.create(address, true) == 0) {
					MTCL_SHM_PRINT(1, "ConnSHM::listen, removed stale endpoint %s\n", address.c_str());
				} else {
					MTCL_SHM_PRINT(100, "ConnSHM::listen ERROR errno=%d (%s)\n", errno, strerror(errno));
					return -1;
				}
			} else {
				MTCL_SHM_PRINT(100, "ConnSHM::listen ERROR errno=%d (%s)\n", errno, strerror(errno));
				return -1;
			}
		}
        MTCL_SHM_PRINT(1, "listening to %s\n", address.c_str());

        return 0;
    }

    void update() {

        REMOVE_CODE_IF(std::unique_lock ulock(shm, std::defer_lock));		
		ssize_t sz=-1;
		if (connbuff.isOpen()) { // we are listening for incoming connections
			// check first for new connections
			if (((sz=connbuff.trygetsize())==-1) && errno!=EAGAIN) {
				MTCL_SHM_ERROR("ConnSHM::update ERROR errno=%d (%s)\n", errno,strerror(errno));
				goto skip;
			}
			if (sz!=-1) { // new connection
				char msg[SHM_SMALL_MSG_SIZE];
				if ((sz=connbuff.get(msg, SHM_SMALL_MSG_SIZE))==-1) {
					MTCL_SHM_ERROR("ConnSHM::update ERROR errno=%d (%s)\n", errno,strerror(errno));
					goto skip;
				}
				msg[sz]='\0';
				auto c = std::string(msg).find(":");
				if (c == std::string::npos) {
					MTCL_SHM_ERROR("ConnSHM::update ERROR invalid message\n");
					goto skip;
				}
				std::string inname  = std::string(msg).substr(0, c);
				std::string outname = std::string(msg).substr(c+1);
				
				shmBuffer in;
				if (in.open(outname)==-1) {
					MTCL_SHM_ERROR("ConnSHM::update, opening %s errno=%d (%s)\n", outname.c_str(), errno, strerror(errno));
					goto skip;
				}
				shmBuffer out;
				if (out.open(inname)==-1) {
					MTCL_SHM_ERROR("ConnSHM::update, opening %s errno=%d (%s)\n", inname.c_str(), errno, strerror(errno));
					goto skip;
				}
				
				auto handle = new HandleSHM(this, in, out);
				REMOVE_CODE_IF(ulock.lock());
				connections.insert({handle, false});
				REMOVE_CODE_IF(ulock.unlock());                    
				addinQ(true, handle);
			}
		}
	skip:
		REMOVE_CODE_IF(ulock.lock());		
        for (auto &[handle, to_manage] : connections) {
            if(to_manage) {
				if (((sz=handle->in.peek())<0)) {
					if (errno!=EWOULDBLOCK)
						MTCL_SHM_ERROR("ConnSHM::update, peek errno=%d (%s)\n", errno, strerror(errno));
					continue;
				}
				// NOTE: called with ulock lock hold. Double lock if there is the IO-thread!
				addinQ(false, handle);
			}
        }
		REMOVE_CODE_IF(ulock.unlock());		
    }

    Handle* connect(const std::string& address, int retry=-1, unsigned timeout=0) {
		shmBuffer connshm;
		if (connshm.open(address) == -1) {
			MTCL_SHM_PRINT(100, "ConnSHM::connect, cannot open the connection buffer, errno=%d\n", errno);
			return nullptr;
		}
		auto id = shmconnid++ % SHM_MAX_CONCURRENT_CONN;
		std::string inname = "/"+shmname+"_in_"+std::to_string(id);
		std::string outname= "/"+shmname+"_out_"+std::to_string(id);

		shmBuffer in;
		// create a buffer for input messages
		if (in.create(inname, false)<0) {
			MTCL_SHM_PRINT(100, "ConnSHM::connect, cannot create input buffer, errno=%d\n", errno);
			return nullptr;
		}
		shmBuffer out;
		// create a buffer for output messages
		if (out.create(outname, false)<0) {
			MTCL_SHM_PRINT(100, "ConnSHM::connect, cannot create output buffer, errno=%d\n", errno);
			return nullptr;
		}
		std::string msg= inname+":"+outname;
		// sending the connection message
		if (connshm.put(msg.c_str(),msg.length())<0) {
			MTCL_SHM_PRINT(100, "ConnSHM::connect, ERROR sending the connect message %s, errno=%d (%s)\n", msg.c_str(), errno, strerror(errno));
			return nullptr;
		}
		
		MTCL_SHM_PRINT(100, "connected to %s, (in=%s, out=%s)\n", address.c_str(), inname.c_str(), outname.c_str());
		
        HandleSHM *handle = new HandleSHM(this, in, out);
		{
			REMOVE_CODE_IF(std::unique_lock lock(shm));
			connections[handle] = false;
		}
        return handle;
    }

    void notify_close(Handle* h, bool close_wr=true, bool close_rd=true) {
		HandleSHM *handle = reinterpret_cast<HandleSHM*>(h);
		if (close_wr) {
			handle->out.close(true);
		}
		if (close_rd) {
			{
				REMOVE_CODE_IF(std::unique_lock lock(shm));
				connections.erase(handle);
			}
			handle->in.close(true);			
		}
    }

    void notify_yield(Handle* h) override {
		REMOVE_CODE_IF(std::unique_lock l(shm));
		auto handle = reinterpret_cast<HandleSHM*>(h);
		auto it = connections.find(handle);
		if (it != connections.end())
			connections[handle] = true;
    }

    void end(bool blockflag=false) {
        auto modified_connections = connections;
        for(auto& [handle, _] : modified_connections) {
			setAsClosed(handle, blockflag);
		}
		connbuff.close(true);
    }

};

} // namespace

#endif
