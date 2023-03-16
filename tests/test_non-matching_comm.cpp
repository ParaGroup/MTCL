/*
 * This test should check if non-matching send/receive operations are supported
 * by the specified protocol.
 * 
 * 
 *
 * Testing MPI:
 * ^^^^^^^^^^^^
 *  $> mpicxx -I .. -std=c++17 -DENABLE_MPI -Wall -o test_non-matching_comm test_non-matching_comm.cpp  -pthread -lrt
 *  $> MTCL_VERBOSE=10 mpirun -n 1 ./test_non-matching_comm 0 server : -n 1 ./test_non-matching_comm 1 client
 * 
 * ===========================
 * MPI hangs if we send a *big* (still not sure how big) message which is not
 * matched by the remote peer.
 * N=7, minsize=16 --> terminates
 * N=8, minsize=16 --> hangs
 * ===========================
 * 
 * 
 * 
 * Testing UCX:
 * ^^^^^^^^^^^^
 *  $> g++ -I .. -std=c++17 -DENABLE_UCX -Wall -o test_non-matching_comm test_non-matching_comm.cpp  -lucp -luct -lucs -lucm -pthread -lrt
 *  $> MTCL_VERBOSE=10 ./test_non-matching_comm 0 server
 *  $> MTCL_VERBOSE=10 ./test_non-matching_comm 1 client
 * */

#include <iostream>
#include "../mtcl.hpp"

const int max_msg_size=100;          // max message size
const std::string bye{"Bye!"};       // bye bye message
const std::string welcome{"Hello!"}; // welcome message

const int          N = 8;
const size_t minsize = 16;              // bytes
const size_t maxsize = (1<<N)*minsize;

int main(int argc, char** argv){
#ifndef ENABLE_MPI
#ifndef ENABLE_UCX
    std::cerr << "You must compile with MPI or UCX support\n";
    return 1;
#endif
#endif

    if(argc < 3) {
		MTCL_ERROR("Usage: ", "%s <0|1> <appName> [receive_all]\n", argv[0]);
        return -1;
    }

    int rank = std::stol(argv[1]);
    Manager::init(argv[2]);

    if (rank == 0){
        Manager::listen("MPI:0:10");
        Manager::listen("UCX:0.0.0.0:21000");
        
        char *buff = new char[maxsize];
	    assert(buff);
	    memset(buff, 'a', maxsize);
        
        auto h = Manager::getNext();
        if (h.isNewConnection()) {
            MTCL_PRINT(0, "[SERVER]:\t", "Received new connection\n");
            // These are matched by the remote peer
            h.send(welcome.c_str(), welcome.length());
            h.send(bye.c_str(), bye.length());
            
            // This is not going to be matched by the remote peer, we already
            // sent the bye message
            h.send(buff, maxsize);
            MTCL_PRINT(0, "[SERVER]:\t", "Sent all messages\n");
            h.close();
        }
    }
    // Client waits for the bye message and terminates its execution
    else {
        
        auto handle = []() {
            auto h = Manager::connect("MPI:0:10");
            if (!h.isValid()) {
                auto h = Manager::connect("UCX:0.0.0.0:21000");
                assert(h.isValid());
                return h;
            } else return h;
        }();

        if(handle.isValid()) {
            MTCL_PRINT(0, "[CLIENT]:\t", "Connected to server\n");

            while(true) {
		        int r=0;
                size_t size=0;
                char buff[max_msg_size+1];

                if ((r=handle.probe(size, true))<=0) {
                    if (r==0) {
                        MTCL_PRINT(10, "[CLIENT]:\t", "The client unexpectedly closed the connection. Bye! (size)\n");
                    } else
                        MTCL_ERROR("[CLIENT]:\t", "ERROR receiving the message size. Bye!\n");
                    handle.close();
                    continue;
                }
                MTCL_PRINT(0, "[CLIENT]:\t", "Incoming message with size %ld\n", size);
                assert(size<max_msg_size); // check
                if ((r=handle.receive(buff, size))<=0) {
                    if (r==0){
                        MTCL_PRINT(10, "[CLIENT]:\t", "The client unexpectedly closed the connection. Bye! (payload)\n");
                    } else
                        MTCL_ERROR("[CLIENT]:\t", "ERROR receiving the message payload. Bye!\n");
                    handle.close();
                    continue;
                }
                buff[size]='\0';
                MTCL_PRINT(0, "[CLIENT]:\t", "Received message \'%s\'\n", buff);
                if (std::string(buff) == bye) {
                    MTCL_PRINT(0, "[CLIENT]:\t", "The server sent the bye message! Goodbye!\n");
                    break;
                }
            }
            handle.close();
        }
    }

    MTCL_PRINT(0, rank ? "[CLIENT]:\t" : "[SERVER]:\t", "Finalizing...\n");
    Manager::finalize();
    MTCL_PRINT(0, rank ? "[CLIENT]:\t" : "[SERVER]:\t", "Finalized\n");
    return 0;

}