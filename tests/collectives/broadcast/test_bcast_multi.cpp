/*
 *
 * Simple broadcast test with two different groups
 *
 * 
 * Compile with:
 *  $> RAPIDJSON_HOME=<rapidjson_install_path> make -f ../Makefile clean test_bcast_multi
 * 
 * Execution:
 *  $> ./test_bcast_multi 0 App1
 *  $> ./test_bcast_multi 1 App2
 *  $> ./test_bcast_multi 2 App3 
 *  $> ./test_bcast_multi 2 App4
 * 
 * 
 */


#include <iostream>
#include "../../../mtcl.hpp"

#define MAX_MESSAGE_SIZE 100

inline static std::string hello{"Hello team!"};
inline static std::string bye{"Bye team!"};
inline static std::string hello_hg2{"Hello team 2!"};

int main(int argc, char** argv){

    if(argc < 2) {
        printf("Usage: %s <0|1> <App1|App2|App3>\n", argv[0]);
        return 1;
    }

    std::string config;
#ifdef ENABLE_TCP
    config = {"tcp_config.json"};
#endif
#ifdef ENABLE_MPI
    config = {"mpi_config.json"};
#endif
#ifdef ENABLE_UCX
    config = {"ucx_config.json"};
#endif

    if(config.empty()) {
        printf("No protocol enabled. Please compile with TPROTOCOL=TCP|UCX|MPI\n");
        return 1;
    }

    int rank = atoi(argv[1]);
	Manager::init(argv[2], config);

    // Root
    if(rank == 0) {
        auto hg = Manager::createTeam("App1:App2:App3:App4", "App1", BROADCAST);
        auto hg2 = Manager::createTeam("App1:App2", "App1", BROADCAST);
        if(!hg.isValid() || !hg2.isValid()) {
            MTCL_PRINT(1, "[test_bcast_multi]:\t", "there was some error creating the teams.\n");
            return 1;
        }

        hg.sendrecv((void*)hello.c_str(), hello.length(), nullptr, 0);
        hg2.sendrecv((void*)hello_hg2.c_str(), hello_hg2.length(), nullptr, 0);
        hg.sendrecv((void*)bye.c_str(), bye.length(), nullptr, 0);

        // hg.close();
        // hg2.close();
    }
    else {
		auto hg = Manager::createTeam("App1:App2:App3:App4", "App1", BROADCAST);
        if(!hg.isValid()) {
            return 1;
        }

        HandleUser hg2;
        if(rank==1) {
            hg2 = Manager::createTeam("App1:App2", "App1", BROADCAST);
            if(!hg2.isValid()) {
                return 1;
            }
        }
        
        char* s = new char[hello.length()+1];
        hg.sendrecv(nullptr, 0, s, hello.length());
        s[hello.length()] = '\0';
        std::cout << "Received: " << s << std::endl;

        hg.sendrecv(nullptr, 0, s, bye.length());
        s[bye.length()] = '\0';
        if(std::string(s) == bye)
            printf("Received bye message: %s\n", bye.c_str());
        delete[] s;

        if(rank == 1) {
            s = new char[hello_hg2.length()+1];
            hg2.sendrecv(nullptr, 0, s, hello_hg2.length());
            s[hello_hg2.length()] = '\0';
            std::cout << "Received: " << s << std::endl;
            delete[] s;
            hg2.close();
        }
        hg.close();
    }

    Manager::finalize(true);

    return 0;
}
