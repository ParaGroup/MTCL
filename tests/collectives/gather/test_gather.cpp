/*
 *
 * Gather implementation test
 *
 *
 * Compile with:
 *  $> TPROTOCOL=<TCP|UCX|MPI> RAPIDJSON_HOME="/rapidjson/install/path" make -f ../Makefile clean test_gather
 * 
 * Execution:
 *  $> ./test_gather App1
 *  $> ./test_gather App2
 *  $> ./test_gather App3
 *  $> ./test_gather App4
 * 
 * Execution with MPI:
 *  $> mpirun -n 1 ./test_gather App1 : -n 1 ./test_gather App2 : -n 1 ./test_gather App3 : -n 1 ./test_gather App4
 * 
 * */

#include <iostream>
#include <string>
#include "mtcl.hpp"

int main(int argc, char** argv){

    if(argc != 2) {
		MTCL_ERROR("[test_gather]:\t", "Usage: %s <App1|App2|...|AppN>\n", argv[0]);
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
		MTCL_ERROR("[test_gather]:\t", "No protocol enabled. Please compile with TPROTOCOL=TCP|UCX|MPI\n");
        return -1;
    }

	Manager::init(argv[1], config);

    auto hg = Manager::createTeam("App1:App2:App3:App4", "App1", MTCL_GATHER);
    if(!hg.isValid()) {
		MTCL_ERROR("[test_gather]:\t", "Error creating the team\n");
		return -1;
	}
    
    std::string data{argv[1]};

    char* buff = nullptr;
	size_t recvsize=hg.size()*data.length();
    if(hg.getTeamRank() == 0) buff = new char[recvsize];
	
    if (hg.sendrecv(data.c_str(), data.length(), buff, recvsize, data.length()) <= 0) {
		MTCL_ERROR("[test_gather]:\t", "sendrecv failed\n");
	}

    hg.close();
	
    // Root
    if(hg.getTeamRank() == 0) {

		for(int i = 0; i < hg.size(); i++) {
            std::string res(buff+(i*data.length()), data.length());
            printf("buff[%d] = %s\n", i, res.c_str());
        }
        delete[] buff;
    }

    Manager::finalize(true);

    return 0;
}
