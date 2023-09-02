/*
 *
 * AlltoaLl implementation test
 *
 *
 * Compile with:
 *  $> TPROTOCOL=<TCP|UCX|MPI> RAPIDJSON_HOME="/rapidjson/install/path" make -f ../Makefile clean test_alltoall
 * 
 * Execution:
 *  $> ./test_alltoall App1 size
 *  $> ./test_alltoall App2 size
 *  $> ./test_alltoall App3 size
 *  $> ./test_alltoall App4 size
 * 
 * Execution with MPI:
 *  $> mpirun -n 1 ./test_alltoall App1 size : -n 1 ./test_alltoall App2 size : -n 1 ./test_alltoall App3 size : -n 1 ./test_alltoall App4 size
 * 
 */

#include <iostream>
#include <string>
#include <mtcl.hpp>

int main(int argc, char** argv){

    if(argc != 3) {
		MTCL_ERROR("[test_alltoall]:\t", "Usage: %s <App1|App2|...|AppN> size\n", argv[0]);
        return -1;
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
		MTCL_ERROR("[test_alltoall]:\t", "No protocol enabled. Please compile with TPROTOCOL=TCP|UCX|MPI\n");
        return -1;
    }

    size_t size = std::stol(argv[2]);

    if ((size / 4) < 1) {
        MTCL_ERROR("[test_alltoall]:\t", "size too small!\n");
        return -1;
	} 

	Manager::init(argv[1], config);

    auto hg = Manager::createTeam("App1:App2:App3:App4", "App1", MTCL_ALLTOALL);
    if(!hg.isValid()) {
		MTCL_ERROR("[test_alltoall]:\t", "Error creating the team\n");
		return -1;
	}
    
    int buff_size = hg.getTeamPartitionSize(size) * 4;
    char* buff = new char[buff_size + 1];

    std::string data (size, argv[1][3]);

    if (hg.sendrecv(data.c_str(), data.length(), buff, buff_size) <= 0) {
		MTCL_ERROR("[test_alltoall]:\t", "sendrecv failed\n");
	}

    hg.close();

    buff[buff_size] = '\0';
    printf("%s-buff = %s\n", argv[1], buff);
    delete [] buff;

    Manager::finalize(true);

    return 0;
}
