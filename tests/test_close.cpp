#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <iostream>
#include "mtcl.hpp"

using namespace MTCL;

int main(int argc, char** argv){

	pid_t pid = fork();
	if (pid == 0) {
		Manager::init("server");
		Manager::listen("TCP:localhost:13000");

		int x;
		auto handle = Manager::getNext();
		if (handle.receive(&x, sizeof(x))<0) {
			MTCL_ERROR("[Server]:", "receive reported errno=%d (%s)\n",
					   errno, strerror(errno));
		}
		handle.close();
		if (handle.send(&x, sizeof(x))<0) {
			MTCL_ERROR("[Server]:", "send after close() reported errno=%d (%s)\n",
					   errno, strerror(errno));
		}
		Manager::finalize();
		int r;
		if ((r=handle.receive(&x, sizeof(x)))<=0) {
			MTCL_ERROR("[Server]:", "receive after finalize() reported r=%d, errno=%d, (%s)\n",
					   r, errno, strerror(errno));
		}
		return 0;
	}
	Manager::init("client");
	auto handle = Manager::connect("TCP:localhost:13000", 10, 200);
	if (!handle.isValid()) {
		MTCL_ERROR("[Client]:", "cannot connect to server, exit\n");
		return -1;
	}
	int x;
	handle.send(&x,sizeof(x));
    Manager::finalize();

	wait(NULL);
    return 0;
}
