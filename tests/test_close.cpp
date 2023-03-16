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

		int x;
		auto handle = Manager::getNext();
		handle.yield();
		handle.receive(&x, sizeof(x));
		handle.close();
		handle.send(&x, sizeof(x));
		Manager::finalize();
		handle.receive(&x, sizeof(x));
		return 0;
	}
	Manager::init("client");
	HandleUser handle;
	for(int i=0;i<5;++i) {
		auto h = Manager::connect("TCP:localhost:13000");
		if (!h.isValid()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(200));
			continue;
		}
		handle = std::move(h);
		break;
	}
	if (!handle.isValid()) {
		MTCL_ERROR("[Client]:\t", "cannot connect to server, exit\n");
		return -1;
	}
	int x;
	handle.send(&x,sizeof(x));
    Manager::finalize();

	wait(NULL);
    return 0;
}
