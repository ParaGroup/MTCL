/*
 *  For maximum performance, set to 0 the *_POLL_TIMEOUT constants in the
 *  config.hpp file and compile the program with SINGLE_IO_THREAD=1.
 *
 *  $> TPROTOCOL="MPI UCX" make SINGLE_IO_THREAD=1 cleanall p2p-perf
 *
 *  When using the MPI transport with the IO_THREAD present, then it is 
 *  important to control thread affinity for example by using something like:
 * 
 *  $> mpirun --mca hwloc_base_binding_policy socket    \
 *          -n 1 --host host1 ./p2p-perf 0 "MPI:0:10" :	\
 *          -n 1 --host host2 ./p2p-perf 1 "MPI:0:10" 
 *
 *  or 
 *  $>  mpirun --report-bindings --bind-to-core .....
 *
 */

#include <cassert>
#include <cmath>
#include <iostream>
#include <iomanip>
#include <vector>
#include "mtcl.hpp"

const int     NROUND = 100;
const int          N = 24;
const size_t minsize = 16;              // bytes
const size_t maxsize = (1<<N);

#define DATACHECK
#if defined(DATACHECK)
#define CHECK(X) {X;}
#else
#define CHECK(X) 
#endif

void Server(const char serveraddr[]) {
	if (Manager::listen(serveraddr) == -1) {
		MTCL_ERROR("[Server]:\t", "listen ERROR -- %s\n", strerror(errno));
		return;
	}

	char *buff = new char[maxsize];
	assert(buff);

	auto handle=Manager::getNext();
	for(size_t size=minsize; size<=maxsize; size *= 2) {
		for(int i=0;i<NROUND;++i) {
			size_t r;
			if ((r=handle.receive(buff, size))<=0) {
				MTCL_ERROR("[Server]:\t", "receive error, errno=%d (%s)\n",
						   errno, strerror(errno));
				break;
			}
			CHECK(buff[i]='b');
			assert(r==size);
			if (handle.send(buff, size)<=0) {
				MTCL_ERROR("[Server]:\t", "send error, errno=%d (%s)\n",
						   errno, strerror(errno));
				break;
			}
		}
	}
	handle.yield();
	
	MTCL_PRINT(0, "[Server]:\t", "closing\n");
	delete [] buff;
	auto h = Manager::getNext();	
	h.close();
}

void Client(const char serveraddr[]) {
	auto handle = Manager::connect(serveraddr, 5, 1000);
	if (!handle.isValid()) {
		MTCL_ERROR("[Client]:\t", "cannot connect to server, exit\n");
		return;
	}

	char *buff = new char[maxsize];
	assert(buff);
	char *buff2 = new char[maxsize];
	assert(buff2);
	memset(buff, 'a', maxsize);

	std::vector<std::pair<double, double>> times;
	
	for(size_t size=minsize; size<=maxsize; size *= 2) {
		std::chrono::microseconds V[NROUND];
		for(size_t i=0;i<NROUND;++i) {
			size_t r;
			auto start = std::chrono::system_clock::now();
			if ((r=handle.send(buff, size))<=0) {
				MTCL_ERROR("[Client]:\t", "send error, errno=%d (%s) i=%d\n",
						   errno, strerror(errno), i);
				break;
			}
			if ((r=handle.receive(buff2, size))<=0) {
				MTCL_ERROR("[Client]:\t", "receive error, errno=%d (%s)\n",
						   errno, strerror(errno));
				break;
			}			
			auto end = std::chrono::system_clock::now();
			assert(r==size);
			CHECK(for(size_t j=0;j<size;++j) { if (i==j) { if (buff2[j]!='b') abort();} else { if (buff2[j]!='a') abort();}})
			V[i] = std::chrono::duration_cast<std::chrono::microseconds>(end-start);
		}
		double sum{0.0};
		for(int i=0;i<NROUND;++i)
			sum += (V[i].count()/2.0);
		double mean{sum/NROUND};
		sum=0.0;
		for(int i=0;i<NROUND;++i) 
			sum += std::pow(V[i].count() - mean, 2);
		times.push_back(std::make_pair(mean, std::sqrt(sum/(NROUND-1))));
	}
	MTCL_PRINT(0, "[Client]:\t", "closing\n");
	handle.close();	
	
	size_t size = minsize;
	std::cout << "   size   lat avg (ms)   lat std (ms)       Bw (MB/s)\n";
	std::cout << "-----------------------------------------------------\n";
	for(auto& p:times) {		
		std::cout << std::fixed << std::setprecision(4)
				  << std::setw(7) << size << "        "
				  << std::setw(6) << p.first/1000.0 << "         "
				  << std::setw(6) << p.second/1000.0 << "        "
				  << std::setw(9) << (size*1e6)/(1048576*p.first) << "\n";
		size *= 2;
	}
	delete [] buff;
	delete [] buff2;
}

int main(int argc, char** argv){
    if(argc < 3) {
		MTCL_ERROR("Usage: ", "%s <0|1> server-addr\n", argv[0]);
        return -1;
    }
    Manager::init(argv[1]);   
    if (std::stol(argv[1]) == 0)
		Server(argv[2]);            
    else
		Client(argv[2]);	
    Manager::finalize(true);
	
    return 0;
}
