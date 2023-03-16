/*
 *
 * Gather implementation test
 *
 *
 * Compile with:
 *  $> TPROTOCOL=<TCP|UCX|MPI> RAPIDJSON_HOME="/rapidjson/install/path" make -f ../Makefile clean test_gather
 * 
 * Execution:
 *  $> ./test_gather 0 App1
 *  $> ./test_gather 1 App2
 *  $> ./test_gather 2 App3
 *  $> ./test_gather 3 App4
 * 
 * Execution with MPI:
 *  $> mpirun -n 1 ./test_gather 0 App1 : -n 1 ./test_gather 1 App2 : -n 1 ./test_gather 2 App3 : -n 1 ./test_gather 3 App4
 * 
 * */

#include <iostream>
#include <string>
#include "../../../mtcl.hpp"

int main(int argc, char** argv){

    if(argc < 3) {
        printf("Usage: %s <0|1> <App1|App2>\n", argv[0]);
        return 1;
    }

    int rank = atoi(argv[1]);

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

    printf("Running with config file: %s\n", config.c_str());
	Manager::init(argv[2], config);

    auto hg = Manager::createTeam("App1:App2:App3:App4", "App1", GATHER);
    if(hg.isValid()) printf("Created team with size: %d\n", hg.size());
    
    std::string data{argv[2]};
    data += '\0';

    char* buff = nullptr;
    if(rank == 0) buff = new char[hg.size()*data.length()];

    hg.sendrecv(data.c_str(), data.length(), buff, data.length());
//     if(rank != 0) {
// #if 1
//         hg.sendrecv(data.c_str(), data.length(), buff, data.length());
// #endif
//     }
    hg.close();

    // Root
    if(rank == 0) {
        for(int i = 0; i < hg.size(); i++) {
            std::string res(buff+(i*data.length()), data.length());
            printf("buff[%d] = %s\n", i, res.c_str());
        }
        delete[] buff;
    }

    Manager::finalize(true);

    return 0;
}
