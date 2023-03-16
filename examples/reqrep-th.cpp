/*
 * Simple test in which the server and the clients use a dedicated thread for sending 
 * messages to the other peer
 *
 * Spawning multiple clients with mpirun: 
 * 
 *  mpirun -n 1 --host localhost:16 -x MTCL_VERBOSE=1 ./reqrep-th 0 0 : 
 *         -n 15 --host localhost:16  -x MTCL_VERBOSE=all ./reqrep-th 1 1
 */

#include <cassert>
#include <iostream>
#include <map>
#include <thread>
#include <mutex>
#include "mtcl.hpp"

using namespace std::chrono_literals;

const int nmsg=100;
const int maxwaittime=10; // milliseconds

void Server() {
	Manager::listen("TCP:0.0.0.0:42000");

	std::mutex mtx;
	std::map<size_t, bool> connections;
	
	auto sender = [&](HandleUser && handle) {
						for(int i=0;i<100;++i) {
							if (handle.send(&i,sizeof(i))<=0) {
								MTCL_ERROR("[SERVER]:\t", "sender ERROR, errno=%d\n", errno);
								continue;
							}
							std::this_thread::sleep_for(std::chrono::milliseconds(rand() % maxwaittime +1));			
						}
						handle.close();
						{
							std::unique_lock lk(mtx);
							connections[handle.getID()] = true;
						}
				  };	
	while(1) {
		auto handle = Manager::getNext();
		if(handle.isNewConnection()) {
			MTCL_PRINT(1, "[SERVER]:\t", "new connection\n");
			{
				std::unique_lock lk(mtx);
				connections[handle.getID()]=false;
			}
			std::thread(sender, std::move(handle)).detach();
			continue;
		}
		int y;
		if (handle.receive(&y, sizeof(int))<=0) {
			std::unique_lock lk(mtx);
			if (connections[handle.getID()]) {
				MTCL_PRINT(10, "[SERVER]:\t", "receiver close handle\n");
				handle.close();
			} 
		} 
	}
}
void Client() {
	auto handle = Manager::connect("TCP:0.0.0.0:42000");
	if (!handle.isValid()) {
		MTCL_ERROR("[CLIENT]:\t", "Client cannot connect\n");
		return;
	}
	auto t = std::thread([&handle]() {
							 for(int i=0;i<100;++i) {
								 if (handle.send(&i,sizeof(i))<=0) {
									 MTCL_ERROR("[CLIENT]:\t", "sender ERROR, errno=%d\n", errno);
								 }
								 std::this_thread::sleep_for(std::chrono::milliseconds(rand()%maxwaittime +1));
							 }
							 MTCL_PRINT(10, "[CLIENT]:\t", "sender terminating\n");
						 });
	handle.yield();
	int y;
	for(int i=0;i<100;++i) {
		auto handle = Manager::getNext();
		if (handle.receive(&y, sizeof(int))<=0) {
			MTCL_ERROR("[CLIENT]:\t", "receive ERROR, errno=%d\n");
			continue;
		}
		if (i!=y)
			MTCL_ERROR("[CLIENT]:\t", "ERROR: received %d expected %d\n", y, i);
	}
	t.join();
	handle.close();
}

int main(int argc, char** argv){
    if(argc < 3) {
		MTCL_ERROR("Usage: ", "%s <0|1> <appName>\n", argv[0]);
        return -1;
    }
	
    Manager::init(argv[2]);   
    if (std::stol(argv[1]) == 0)
		Server();            
    else
		Client();	
    Manager::finalize();
	
    return 0;
}
