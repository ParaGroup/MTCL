/*
 * Simple "hello world" client-server example.
 * To run the server:  ./hello_world 0
 * To run the client:  ./hello_world 1
 * 
 * Testing TCP:
 * ^^^^^^^^^^^^
 *   $> make cleanall hello_world
 *   $> ./hello_world 0 server &
 *   $> for i in {1..4}; do ./hello_world 1 client$i & done
 *   $> pkill -HUP hello_world
 *
 * Testing (plain) MPI:
 * ^^^^^^^^^^^^^^^^^^^^
 *   $> TPROTOCOL=MPI make cleanall hello_world
 *   $> mpirun -n 1 ./hello_world 0 server : -n 3 ./hello_world 1 client &
 *   $> sleep 3; pkill -HUP hello_world
 *
 * Testing MPIP2P (point-to-point MPI):
 * ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 *   $> TPROTOCOL=MPIP2P make cleanall hello_world ../protocols/stop_accept
 *   $> ${MPI_HOME}/bin/ompi-server --report-uri uri_file.txt
 *   $> mpirun -n 1 --ompi-server file:uri_file.txt ./hello_world 0 server &
 *   $> mpirun -n 1 --ompi-server file:uri_file.txt ./hello_world 1 client
 *   $> pkill -HUP hello_world
 *   $> pkill -HUP ompi-server
 *   BUG: currently plain MPI processes cannot connect using MPIP2P 
 *
 *  FIX: non va anche con mpirun separate.......
 *
 * Testing MQTT:
 * ^^^^^^^^^^^^^
 *   $> PAHO_HOME=<mqtt-client-home> TPROTOCOL=MQTT make cleanall hello_world
 *   $> mosquitto -v (it could be already running if installed with apt)
 *   $> ./hello_world 0 server
 *   $> for i in {1..4}; do ./hello_world 1 "client$i" & done
 *
 * Testing UCX:
 * ^^^^^^^^^^^^
 *   For a list of all network configurations, refer to:
 *   https://openucx.readthedocs.io/en/master/faq.html#network-capabilities
 *   
 *   $> TPROTOCOL=UCX make cleanall hello_world
 *   $> [UCX_TLS=tcp,sm] ./hello_world 0 server &
 *   $> for i in {1..4}; do ./hello_world 1 client$i & done
 *
 */

#include <csignal>
#include <cassert>
#include <iostream>
#include "mtcl.hpp"

using namespace std::chrono_literals;

const std::string welcome{"Hello!"}; // welcome message
const std::string bye{"Bye!"};       // bye bye message
const int max_msg_size=100;          // max message size
// for a gentle exit 
static volatile std::sig_atomic_t stop=0;
void signal_handler(int) { stop=1; }

// It waits for new connections, sends a welcome message to the connected client,
// then echoes the input message to the client up to the bye message.
void Server() {
	// Some of the following calls might fail, but at least one will succeed
	Manager::listen("SHM:/MTCA-server");
	Manager::listen("TCP:0.0.0.0:42000");
	Manager::listen("MPI:0:10");
	Manager::listen("MPIP2P:test");
    Manager::listen("MQTT:label");
    Manager::listen("UCX:0.0.0.0:21000");

	char buff[max_msg_size+1];
	while(!stop) {
		// Is there something ready?
		auto handle = Manager::getNext(300ms);
		if (!handle.isValid()) { // timeout expires
			MTCL_PRINT(10, "[SERVER]:\t", "timeout expires\n");
			continue; 
		}

		// Yes. Is it a new connection?
		if(handle.isNewConnection()) {
			// yes it is, sending the welcome message
			handle.send(welcome.c_str(), welcome.length());
			continue;
		}
		// it is not a new connection, sending back the message to the client.
		// We read first the message size (an int) and then the payload.
		int r=0;
		size_t size=0;
		if ((r=handle.probe(size, true))<=0) {
			if (r==0) {
				MTCL_PRINT(10, "[SERVER]:\t", "The client unexpectedly closed the connection. Bye! (size)\n");
			} else
				MTCL_ERROR("[SERVER]:\t", "ERROR receiving the message size. Bye!\n");
			handle.close();
			continue;
		}
		assert(size<max_msg_size); // check
		if ((r=handle.receive(buff, size))<=0) {
			if (r==0){
				MTCL_PRINT(10, "[SERVER]:\t", "The client unexpectedly closed the connection. Bye! (payload)\n");
			} else
				MTCL_ERROR("[SERVER]:\t", "ERROR receiving the message payload. Bye!\n");
			handle.close();
			continue;
		}
		buff[size]='\0';
		if (std::string(buff) == bye) {
			MTCL_PRINT(0, "[SERVER]:\t", "The client sent the bye message! Goodbye!\n");
			handle.close();
			continue;
		}		
		if ((r=handle.send(buff, r))<=0) {
			if (r==0) {
				MTCL_PRINT(10, "[SERVER]:\t", "The client unexpectedly closed the connection. Bye! (reply)\n");
			} else
				MTCL_ERROR("[SERVER]:\t", "ERROR sending the message back to the client, close handle, errno=%d\n", errno);
			handle.close();
		}
	}
	MTCL_PRINT(0,"[SERVER]:\t", "Goodbye!\n");
}
// It connects to the server waiting for the welcome message. Then it sends the string
// "ciao" incrementally to the server, receiving each message back from the server.
void Client() {
	char buff[10];
	// try to connect with different transports until we find a valid one
	auto handle = []() {
					  auto h = Manager::connect("MPIP2P:test");
					  if (!h.isValid()) {
						  auto h = Manager::connect("MPI:0:10");
						  if (!h.isValid()) {
							  auto h = Manager::connect("MQTT:label");
                              if(!h.isValid()) {
                                auto h = Manager::connect("UCX:0.0.0.0:21000");
                                if (!h.isValid()) {
                                    auto h = Manager::connect("TCP:0.0.0.0:42000");
									//auto h = Manager::connect("SHM:/MTCA-server");
                                    assert(h.isValid());
                                    return h;
                                } else return h;
                              } else return h;
						  } else return h;
					  } else return h;
				  }();
	do {
		// wait for the welcome message
		if(handle.receive(buff, welcome.length())<=0) break;
		buff[0]='c'; buff[1]='i'; buff[2]='a';
		buff[3]='o'; buff[4]='!';buff[5]='\0';

		ssize_t res;
		// now sending the string "ciao" incrementally
		for(int i=1;i<=5;++i) {
			if ((res=handle.send(buff, i))<=0) {
				MTCL_ERROR("[CLIENT]:\t", "ERROR sending buffer\n");
				break;
			}
			char rbuf[i+1];
			if ((res=handle.receive(rbuf, i))<=0) {
				if (res==0) {
					MTCL_PRINT(10, "[CLIENT]:\t", "The server unexpectedly closed the connection\n");
				} else 
					MTCL_ERROR("[CLIENT]:\t", "ERROR receiving reply, errno=%d\n", errno);
				break;
			}
			rbuf[i]='\0';
			std::cout << "Read: \"" << rbuf << "\"\n" << std::flush;
		}
		// we can just close the handle here, but we are polite and say Bye!
		if ((res=handle.send(bye.c_str(), bye.length()))<=0) {
			MTCL_ERROR("[CLIENT]:\t", "ERROR sending bye message\n");
			break;
		}
	} while(false);
	handle.close();
}

int main(int argc, char** argv){
    if(argc < 3) {
		MTCL_ERROR("Usage: ", "%s <0|1> <appName>\n", argv[0]);
        return -1;
    }
	std::signal(SIGHUP,  signal_handler);	

    Manager::init(argv[2]);   
    if (std::stol(argv[1]) == 0)
		Server();            
    else
		Client();	
    Manager::finalize();
	
    return 0;
}
