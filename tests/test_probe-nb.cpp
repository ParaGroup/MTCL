#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <iostream>
#include "mtcl.hpp"

int main(int argc, char** argv){

	pid_t pid = fork();
	if (pid == 0) {
		Manager::init("server");
		Manager::listen("TCP:localhost:13000");

		auto handle = Manager::getNext();		
		sleep(0.5);
		char buff[] = "Hello world!";
		handle.send(buff, strlen(buff));
		
		Manager::finalize();
		return 0;
	}
	Manager::init("client");
	HandleUser handle;
	for(int i=0;i<5;++i) {
		auto h = Manager::connect("TCP:localhost:13000");
		if (!h.isValid()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			continue;
		}
		handle = std::move(h);
		break;
	}
	if (!handle.isValid()) {
		MTCL_ERROR("[Client]:\t", "cannot connect to server, exit\n");
		return -1;
	}

	size_t sz=0;
	ssize_t r;
	do {
		r = handle.probe(sz, false);
	} while(r==-1 && errno==EWOULDBLOCK);
	if (r>0) {
		handle.probe(sz, true); // not needed, just for testing
		
		char buff[sz+1];
		handle.receive(buff, sz);
		buff[sz]='\0';
		if (std::string(buff) != "Hello world!") {
			MTCL_ERROR("[test_probe]:\t", "ERROR!\n");
			return -1;
		} else
			MTCL_ERROR("[test_probe]:\t", "OK!\n");		
	}
	handle.close();
    Manager::finalize();

	wait(NULL);
    return 0;
}
