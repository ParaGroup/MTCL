#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <iostream>
#include "mtcl.hpp"

const std::string str{"hello world!"}; //...........................................................................................abcdef.............................................................................ghijk..........................................................................................................................................................................................................012345..........................................................................................................................................................................................................6789.............................................................xywz................................................bye!"};

std::string address{"TCP:localhost:13000"};

int main(int argc, char** argv){
	if (argc>1) {
		address=argv[1];
	}
	
	pid_t pid = fork();
	if (pid == 0) {
		Manager::init("test_connect-server");
		if (Manager::listen(address.c_str())==-1) {
			MTCL_ERROR("[Server]:\t", "ERROR, cannot listen to %s, errno=%d\n", address.c_str(), errno);
			Manager::finalize();
			return -1;
		}
			
		auto h = Manager::getNext();
		if (!h.isNewConnection()) {
			MTCL_ERROR("[Server]:\t", "ERROR, expected a new connection\n");
			Manager::finalize();
			return -1;
		}
		
		if (h.send(str.c_str(), str.length()+1)<0) {
			MTCL_ERROR("[Server]:\t", "ERROR sending string errno=%d\n", errno);
			Manager::finalize();
			return -1;
		} 
		MTCL_PRINT(0, "[Server]:\t", "closing\n");
		h.close();
		Manager::finalize();
		return 0;
	}

	Manager::init("test_connect-client");
	HandleUser handle;
	for(int i=0;i<10;++i) {
		auto h = Manager::connect(address.c_str());
		if (!h.isValid()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			continue;
		}
		handle = std::move(h);
		break;
	}
	if (!handle.isValid()) {
		MTCL_ERROR("[Client]:\t", "cannot connect to server, exit\n");
		Manager::finalize();
		return -1;
	}
	MTCL_PRINT(0, "[Client]:\t", "connected\n");
	
	char buff[str.length()+100];
	if (handle.receive(buff, sizeof(buff))<0) {
		MTCL_ERROR("[Client]:\t", "ERROR receive: errno=%d\n", errno);
	}
	if (str != buff)
		MTCL_ERROR("[Client]:\t", "ERROR receiving the string\n");
	
	handle.yield(); // release the handle
	
	// waiting for close
	auto h = Manager::getNext();
	char c;
	ssize_t r= h.receive(&c, 1);
	if (r!=0)
		MTCL_ERROR("[test_connect]:\t", "ERROR\n");
	
	h.close();
	// NOTE: no yield here!
	
	Manager::finalize();
	
	wait(NULL);
	MTCL_ERROR("[test_connect]:\t", "OK!\n");
    return 0;
}
