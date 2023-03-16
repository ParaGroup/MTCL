/*
 *
 * Simple broadcast test with two different groups
 *
 * 
 * Compile with:
 *  $> TPROTOCOL=<TCP|UCX|MPI> [UCX_HOME="ucx_path" UCC_HOME="ucc_path"] RAPIDJSON_HOME=<rapidjson_install_path> make -f ../Makefile clean test_bcast
 * 
 * Execution:
 *  $> ./test_bcast 0 App1
 *  $> ./test_bcast 1 App2
 * 
 */


#include <iostream>
#include "../../../mtcl.hpp"

#define MAX_MESSAGE_SIZE 100

inline static std::string hello{"Hello team!"};
inline static std::string bye{"Bye team!"};

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

    auto hg = Manager::createTeam("App1:App2", "App1", BROADCAST);
    if(!hg.isValid()) {
        MTCL_PRINT(1, "[test_bcast]:\t", "error creating team [%s].\n", hg.getName());
        return 1;
    }

    char *sendbuff{nullptr}, *recvbuff{nullptr};
    size_t sendsize{0}, recvsize{0};

    if(rank == 0) {
        sendbuff = (char*)hello.c_str();
        sendsize = hello.length();
    }
    else {
        recvsize = hello.length();
        recvbuff = new char[recvsize+1];
    }

    ssize_t res;
    if((res = hg.sendrecv(sendbuff, sendsize, recvbuff, recvsize)) <= 0) {
        printf("Fatal error\n");
        printf("res is: %ld - errno: %d\n", res, errno);
        return 1;
    }
    if(rank == 0) {
#if 1
        hg.sendrecv((void*)hello.c_str(), hello.length(), nullptr, 0);
#endif
    }
    else {
        recvbuff[hello.length()] = '\0';
        std::cout << "Received: " << recvbuff << std::endl;
        delete[] recvbuff;
    }
    
    hg.close();
    Manager::finalize(true);

    return 0;
}
