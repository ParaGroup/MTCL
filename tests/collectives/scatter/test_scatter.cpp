/*
 *
 * Simple Scatter test
 *
 *
 * Compile with:
 *  $> TPROTOCOL=<TCP|UCX|MPI> RAPIDJSON_HOME="/rapidjson/install/path" make clean test_scatter
 * 
 * Execution with MPI:
 *  $> mpirun -n 1 ./test_scatter size App1 : -n 1 ./test_scatter 1 size App2 : -n 1 ./test_scatter size App3 : -n 1 ./test_scatter size App4
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

    if(argc != 3) {
		MTCL_ERROR("[test_scatter]:\t", "Usage: %s size <App1|App2|...|AppN>\n", argv[0]);
        return -1;
    }

	int size = atoi(argv[1]);
	
    std::string config={"config.json"};
	Manager::init(argv[2], config);

    auto hg = Manager::createTeam("App1:App2:App3:App4:App5", "App1", MTCL_SCATTER);
    if(!hg.isValid()) {
		MTCL_ERROR("[test_scatter]:\t", "Cannot create the scatter team\n");
		return -1;
	}
	int rank = hg.getTeamRank();
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

	int recvsize = hg.getTeamPartitionSize(size);
	data_t *buff = new data_t[recvsize]{0.0,"null"};
	
    if (hg.sendrecv(data, sndsize, buff, recvsize*sizeof(data_t), sizeof(data_t)) <= 0) {
		MTCL_ERROR("[test_scatter]:\t", "sendrecv failed\n");
	}

    hg.close();

	// just for having a "clean" printing to the stdout
	usleep(rank * 300000);
	std::cout << "rank: " << rank << "\n";
	for(int i=0; i<recvsize; ++i) 
		std::cout << "[" << buff[i].x << ", " << buff[i].str << "] ";
	std::cout << "\n";

    Manager::finalize(true);

    return 0;
}
