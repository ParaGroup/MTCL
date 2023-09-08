/*
 *
 * Simple broadcast test with two different groups
 *
 * 
 * Compile with:
 *  $> TPROTOCOL=<TCP|UCX|MPI> [UCX_HOME="ucx_path" UCC_HOME="ucc_path"] RAPIDJSON_HOME=<rapidjson_install_path> make -f ../Makefile clean test_bcast
 * 
 * Execution:
 *  $> ./test_bcast App1
 *  $> ./test_bcast App2
 *  $> ./test_bcast App3
 *  $> ./test_bcast App4
 * 
 */


#include <iostream>
#include <mtcl.hpp>

#define MAX_MESSAGE_SIZE 100

inline static std::string hello{"Hello team!"};

int main(int argc, char** argv){

    if(argc != 2) {
        printf("Usage: %s <App1|App2|App3>\n", argv[0]);
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

	Manager::init(argv[1], config);

    auto hg = Manager::createTeam("App1:App2:App3:App4", "App1", MTCL_BROADCAST);
    if(!hg.isValid()) {
        MTCL_PRINT(1, "[test_bcast]:\t", "error creating team [%s].\n", hg.getName());
        return 1;
    }

    char *sendbuff{nullptr}, *recvbuff{nullptr};
    size_t sendsize{0}, recvsize{0};

	recvsize = hello.length();
	recvbuff = new char[recvsize+1];
    if(hg.getTeamRank() == 0) {
        sendbuff = (char*)hello.c_str();
        sendsize = hello.length();
    }

    ssize_t res;
    if((res = hg.sendrecv(sendbuff, sendsize, recvbuff, recvsize)) <= 0) {
        printf("Fatal error\n");
        printf("res is: %ld - errno: %d\n", res, errno);
        return 1;
    }
	recvbuff[hello.length()] = '\0';
	std::cout << hg.getTeamRank() << " received: " << recvbuff << std::endl;
	delete[] recvbuff;
    
    hg.close();
    Manager::finalize(true);

    return 0;
}
