/*
 * See master.cpp file to know how to run one master and multiple workers.
 *
 */
#include <cassert>
#include <iostream>
#include <string>
#include <fstream>
#include <algorithm>

#include <mtcl.hpp>

#undef EXPLICIT_MSG_SIZE

#if defined(EXPLICIT_MSG_SIZE)
const int headersize = 3;
#else
const int headersize = 0;
#endif
const int maxpayload = 100; 

int main(int argc, char** argv){
    Manager::init("client");

	int myid=0;	
	std::ifstream input("workers.list");
    for( std::string line; getline( input, line ); ){
		if (line.empty()) continue;
		if (Manager::listen(line) == 0) break;
		++myid;
    }
	input.close();
	size_t nmsgs=0;
	bool EOSreceived=false;
    while(!EOSreceived){
        auto h = Manager::getNext();
        if (h.isNewConnection()) {
			MTCL_PRINT(10, "[Client]:\t", "worker%d has got a new connection\n", myid);
			continue;
		}
		ssize_t r;
#if defined(EXPLICIT_MSG_SIZE)		
        char len[headersize+1];
        if ((r=h.receive(len, headersize)) == -1) {
			MTCL_ERROR("[Client]:\t", "ERROR receiving the header errno=%d\n", errno);
			h.close();
			break;
		}
		if (r==0) {
			MTCL_ERROR("[Client]:\t", "ERROR unexpected connection close (1)\n");
			h.close();
			break;
		}			
		len[headersize]='\0';
		size_t size = std::stoi(len);
#else
		size_t size;
		if (h.probe(size, true)<=0) {
			MTCL_ERROR("[Client]:\t", "ERROR receiving the header errno=%d\n", errno);
			h.close();
			break;
		}
#endif		
		char buff[size+1];
		if ((r=h.receive(buff, size)) == -1) {
			MTCL_ERROR("[Client]:\t", "ERROR receiving the payload errno=%d\n", errno);
			h.close();
			break;
		}
		if (r==0 || (size_t)r<size) {
			MTCL_ERROR("[Client]:\t", "ERROR unexpected connection close (2)\n");
			h.close();
			break;
		}			
		buff[size]='\0';

		if (std::string(buff) == "EOS") {
			MTCL_PRINT(10, "[Client]:\t", "worker%d got EOS, closing!\n", myid);
			EOSreceived = true;
			h.close();
		} else {
			MTCL_PRINT(1, "[Client]:\t", "worker%d received '%s'\n", myid, buff);
			++nmsgs;
			if (h.send(&buff[0], 1) == -1) {
				MTCL_ERROR("[Client]:\t", "ERROR sending the ack errno=%d\n", errno);
				h.close();
				break;
			}
		}
    }
    Manager::finalize();
	std::cout << "worker" << myid << " received " << nmsgs << " messages\n";
    return 0;
}
