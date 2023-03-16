/*
 * Simple ping-pong example between a server and one (or more) clients. 
 * All connections take place with the same protocol declared at compilation
 * time through TPROTOCOL env variable.
 * 
 * - The clients create a connection toward the server and send a "ping" 
 *   message, then they wait for a reply and close the connection.
 * 
 * - The server expects to receive a fixed number of connections, 
 *   replies with the same message to all the requests and terminate after 
 *   all the clients have closed their endpoint.
 *   The number of accepted connections can be tweaked via the 
 *	 MAX_NUM_CLIENTS macro in this file. After this amount of connections 
 *   the server stops accepting new connections and terminates.
 * 
 * === Compilation ===
 * 
 * make pingpong TPROTOCOL=<protocol_name>
 * 
 * TCP:     make pingpong TPROTOCOL=TCP
 * MQTT:    make pingpong TPROTOCOL=MQTT
 * MPI:     make pingpong TPROTOCOL=MPI
 * MPIP2P   make pingpong TPROTOCOL=MPIP2P
 * 
 * === Execution ===
 * TCP:
 *  - start pingpong server: ./pingpong 0
 *  - start pingpong client: ./pingpong 1
 * 
 * MQTT:
 *  - start mosquitto broker: mosquitto -v
 *  - start pingpong server: ./pingpong 0
 *  - start pingpong client: ./pingpong 1
 * 
 * MPIP2P:
 *  - start ompi-server: 
 *      ompi-server --report-uri uri_file.txt --no-daemonize
 *  - start pingpong server: 
 *      mpirun -n 1 --ompi-server file:uri_file.txt ./pingpong 0
 *  - start pingpong client: 
 *      mpirun -n 1 --ompi-server file:uri_file.txt ./pingpong 1
 * 
 * MPI:
 *  - start pingpong server/client as MPMD: 
 *      mpirun -n 1 ./pingpong 0 : -n 4 ./pingpong 1
 * 
 */

#include <iostream>
#include "mtcl.hpp"

#define MAX_NUM_CLIENTS 4

int main(int argc, char** argv){

    if(argc < 2) {
        printf("Usage: %s <rank>\n", argv[0]);
        return 1;
    }

    std::string listen_str{};
    std::string connect_str{};


#ifdef ENABLE_MPIP2P
    listen_str = {"MPIP2P:published_label"};
    connect_str = {"MPIP2P:published_label"};
#endif

#ifdef ENABLE_TCP
    listen_str = {"TCP:0.0.0.0:42000"};
    connect_str = {"TCP:0.0.0.0:42000"};
#endif

/*NOTE: MPI has no need to call the listen function. We build the listen_str
        to make the Manager happy in this "protocol-agnostic" example.
*/
#ifdef ENABLE_MPI
    listen_str = {"MPI:"};
    connect_str = {"MPI:0:5"};
#endif


#ifdef ENABLE_MQTT
    listen_str = {"MQTT:0"};
    connect_str = {"MQTT:0:app0"};
#endif


#ifdef ENABLE_UCX
    listen_str = {"UCX:0.0.0.0:42000"};
    connect_str = {"UCX:0.0.0.0:42000"};
#endif

    int rank = atoi(argv[1]);
	Manager::init(argv[1]);

    // Listening for new connections, expecting "ping", sending "pong"
    if(rank == 0) {
        Manager::listen(listen_str);

        int count = 0;
        while(count < MAX_NUM_CLIENTS) {

            auto handle = Manager::getNext();

            if(handle.isNewConnection()) {             
                char buff[5];
                if(handle.receive(buff, sizeof(buff)) == 0) {
                    MTCL_PRINT(0, "[Server]:\t", "Connection closed by peer\n");
				} else {
					MTCL_PRINT(0, "[Server]:\t", "Received \"%s\"\n", buff);
				}
                char reply[5]{'p','o','n','g','\0'};
                if (handle.send(reply, sizeof(reply)) != 5) {
					MTCL_ERROR("[Server]:\t", "ERROR sending pong message\n");
				} else {
					MTCL_PRINT(0, "[Server]:\t", "Sent: \"%s\"\n",reply);
				}
            }
            else {
                size_t sz;
                handle.probe(sz);
                if(sz == 0) {
				    MTCL_PRINT(0, "[Server]:\t", "Connection closed by peer\n");
				    handle.close();
				    count++;
                }

			}
        }
    }
    // Connecting to server, sending "ping", expecting "pong"
    else {
		bool connected=false;
		for(int i=0;i<10;++i) {
			auto h = Manager::connect(connect_str);
			if(h.isValid()) {
				char buff[5]{'p','i','n','g','\0'};
				if (h.send(buff, sizeof(buff)) != 5) {
					MTCL_ERROR("[Client]:\t", "ERROR sending ping message\n");
					break;
				}
				MTCL_PRINT(0, "Client]:\t", "Sent: \"%s\"\n", buff);
				connected=true;
				break;
			} else { // implicit handle.yield() when going out of scope
				MTCL_PRINT(0, "Client]:\t", "connection failed\n");
				std::this_thread::sleep_for(std::chrono::seconds(1));
				MTCL_PRINT(0, "Client]:\t", "retry....\n");
			}
		}
		if (!connected) {
			MTCL_PRINT(0, "[Client]:\t", "unable to connect to the server, exit!\n");
			Manager::finalize();
			return -1;
		}

        auto handle = Manager::getNext();
        char buff[5];
		size_t r;
        
        if((r=handle.receive(buff, sizeof(buff))) == 0)
            MTCL_ERROR("[Client]:\t", "Connection has been closed by the server.\n");
        else {
			if (r != sizeof(buff)) {
				MTCL_ERROR("[Client]:\t", "ERROR receiving pong message\n");
			} else {
				MTCL_PRINT(0, "[Client]:\t", "Received: \"%s\"\n", buff);
			}
            handle.close();
			MTCL_PRINT(0, "[Client]:\t", "Connection closed locally, notified the server.\n");
        }
    }

    Manager::finalize();

    return 0;
}
