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

		size_t sz;
		for(int i=0;i<10;++i) {
			auto h = Manager::getNext();
			h.probe(sz, true);
			h.yield();
		}
		auto handle = Manager::getNext();
		char buff[sz+1];
		handle.receive(buff, sz);
		buff[sz]='\0';
		if (std::string(buff) != "Hello world!") {
			MTCL_ERROR("[test_probe]:\t", "ERROR!\n");
			return -1;
		} else
			MTCL_ERROR("[test_probe]:\t", "OK!\n");
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
	char buff[]="Hello world!";
	handle.send(buff, sizeof(buff));
	handle.close();
    Manager::finalize();

	wait(NULL);
    return 0;
}
