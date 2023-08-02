/*
 *
 * Simple Scatter test
 *
 *
 * Compile with:
 *  $> TPROTOCOL=<TCP|UCX|MPI> RAPIDJSON_HOME="/rapidjson/install/path" make -f ../Makefile clean test_scatter
 * 
 * Execution with MPI:
 *  $> mpirun -n 1 ./test_scatter 0 size App1 : -n 1 ./test_scatter 1 size App2 : -n 1 ./test_scatter 2 size App3 : -n 1 ./test_scatter 3 size App4
 * 
 * */

#include <iostream>
#include <string>
#include "mtcl.hpp"

struct data_t {
	float x;
	char  str[10];
};

int main(int argc, char** argv){

    if(argc != 4) {
		MTCL_ERROR("[test_scatter]:\t", "Usage: %s <0|1> size <App1|App2>\n", argv[0]);
        return -1;
    }

    int rank = atoi(argv[1]);
	int size = atoi(argv[2]);
	
    std::string config={"config.json"};
	Manager::init(argv[3], config);

    auto hg = Manager::createTeam("App1:App2:App3:App4", "App1", MTCL_SCATTER);
    if(hg.isValid()) printf("Created team with size: %d\n", hg.size());

	data_t *data = nullptr;

	if (rank==0) {
		data = new data_t[size];

		for(int i=0;i<size;++i) {
			data[i].x = i*3.14;
			strncpy(data[i].str,
					std::to_string(data[i].x).c_str(), 10);
		}
	}

	size_t sndsize = size*sizeof(data_t);

	int p = size / hg.size();
	int r = size % hg.size();

	size_t recvsize = p;
	if (r && rank<r) recvsize++;

	data_t *buff = new data_t[recvsize]{0.0,""};
	
    if (hg.sendrecv(data, sndsize, buff, recvsize*sizeof(data_t), sizeof(data_t)) <= 0) {
		MTCL_ERROR("[test_scatter]:\t", "sendrecv failed\n");
	}

    hg.close();

	// just for having a "clean" printing to the stdout
	usleep(rank * 500000);
	std::cout << "rank: " << rank << "\n";
	for(size_t i=0; i<recvsize; ++i) 
		std::cout << "[" << buff[i].x << ", " << buff[i].str << "] ";
	std::cout << "\n";

    Manager::finalize(true);

    return 0;
}
