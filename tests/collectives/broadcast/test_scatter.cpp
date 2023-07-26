/*
 *
 * Simple scatter test
 * 
 * Compile with:
 *  $> TPROTOCOL=MPI RAPIDJSON_HOME=<rapidjson_install_path> make -f ../Makefile clean test_scatter
 * 
 * Execution:
 *  $> mpirun -n 1 test_scatter 0 App1 : -n 1 test_scatter 1 App2
 * 
 */

#include <iostream>
#include <mtcl.hpp>

#define NUM_ELEMS 10

int main(int argc, char** argv){

    if(argc < 2) {
        printf("Usage: %s <0|1> <App1|App2>\n", argv[0]);
        return 1;
    }

    std::string config;
#ifdef ENABLE_MPI
    config = {"mpi_config.json"};
#endif

    if(config.empty()) {
        printf("No protocol enabled. Please compile with TPROTOCOL=MPI\n");
        return 1;
    }

    int rank = atoi(argv[1]);
	Manager::init(argv[2], config);

    auto hg = Manager::createTeam("App1:App2", "App1", SCATTER);
    if(!hg.isValid()) {
        MTCL_PRINT(1, "[test_scatter]:\t", "error creating team [%s].\n", hg.getName());
        return 1;
    }

    int *sendbuff{nullptr}, *recvbuff{nullptr}, count{NUM_ELEMS / 2};
    size_t sendsize{NUM_ELEMS * sizeof(int)}, recvsize{0};

    if(rank == 0) {
        sendbuff = new int[NUM_ELEMS];

        std::cout << "Message" << ": [ ";

        for (int i = 0, j = 100; i < NUM_ELEMS; i++, j += 100) {
            sendbuff[i] = j;
            std::cout << sendbuff[i] << " ";
		}

        std::cout << "]" << std::endl;
    }

    recvsize = count * sizeof(int);
    recvbuff = new int[count];
      
    ssize_t res;
    if((res = hg.sendrecv(sendbuff, sendsize, recvbuff, recvsize, sizeof(int))) <= 0) {
        printf("Fatal error\n");
        printf("res is: %ld - errno: %d\n", res, errno);
        return 1;
    }


    std::cout << "Receive-" << rank << ": [ ";

    for (int i = 0; i < count; i++) {
        std::cout << recvbuff[i] << " ";
	}

    std::cout << "]" << std::endl;

    
    hg.close();
    Manager::finalize(true);

    return 0;
}
