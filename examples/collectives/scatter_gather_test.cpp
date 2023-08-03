/*
 *
 * scatter_gather_test testing MTCL collectives implementation. 
 * This test is focused on the Scatter and Gather collectives.
 * The test consists of 4 processess and 3 of them chosen by the user,
 * use the scatter or gather collettive to exchange messages. 
 * 
 * The application structure is as follows:
 * 
 * Node1    |   Node2   |   Node3   |   Node4
 * 
 * Es. Scatter group -> (Node1 [root], Node3, Node4)
 * Es. Gather group -> (Node2 [root], Node4, Node3)
 *
 * Compile with:
 *  $> TPROTOCOL=TCP|MPI RAPIDJSON_HOME="<rapidjson_path>" make clean scatter_gather_test.cpp
 * 
 * Execute with:
 *  $> ./scatter_gather_test <node_id> <collective_type> <message_size> <root_node_id> <node_id> <node_id>
 * 
 * Parameters are:
 *   - id: specify the rank of the node
 *   - collective_type: specify scatter collective (0) or gather collective (1)
 *   - message_size: is the number of integers send or receive by root node
 *   - root_node_id: specify the root process
 * 	 - node_id: specify a participant in the collective
 *
 * Execution example with scatter collective of (Node1 [root], Node3, Node4) and size of 8 elements
 *  $> ./scatter_gather_test 0 0 8 0 2 3
 *  $> ./scatter_gather_test 1 0 8 0 2 3
 *  $> ./scatter_gather_test 2 0 8 0 2 3 
 *  $> ./scatter_gather_test 3 0 8 0 2 3
 * 
 * or using mpirun and the MPMD model:
 *  $> mpirun -x MTCL_VERBOSE="all" -n 1 ./scatter_gather_test 0 0 8 0 2 3 : \
 *            -x MTCL_VERBOSE="all" -n 1 ./scatter_gather_test 1 0 8 0 2 3 : \
 *            -x MTCL_VERBOSE="all" -n 1 ./scatter_gather_test 2 0 8 0 2 3 : \
 *            -x MTCL_VERBOSE="all" -n 1 ./scatter_gather_test 3 0 8 0 2 3 
 * 
 */

#include <iostream>
#include "mtcl.hpp"

int main(int argc, char* argv[]){

    if(argc < 7) {
		std::cout << "Usage: " << argv[0] << " <node_id> <collective_type> <message_size> <root_node_id> <node_id> <node_id>\n";
        return 1;
    }

	int rank = atoi(argv[1]);
	int type = atoi(argv[2]);
	int size = atoi(argv[3]);
	int node_root = atoi(argv[4]);
	int node_1 = atoi(argv[5]);
	int node_2 = atoi(argv[6]);
	
    std::string config{"scatter_gather_test.json"};
	std::string appName = "Node" + std::to_string(rank);

	Manager::init(appName, config);

	if (rank == node_root || rank == node_1 || rank == node_2) {
		std::string participants = "Node" + std::to_string(node_root) + ":Node" + std::to_string(node_1) + ":Node" + std::to_string(node_2);
		std::string root = "Node" + std::to_string(node_root);

		HandleUser hg;

		if (type == 0) // Scatter
			hg = Manager::createTeam(participants, root, MTCL_SCATTER);
		else
			hg = Manager::createTeam(participants, root, MTCL_GATHER);

		if(!hg.isValid()) {
			MTCL_ERROR("[scatter_gather_test]:\t", "Manager::createTeam for SCATTER, ERROR\n");
			return -1;
		}

		int *data = nullptr;

		if (rank == node_root) {
			data = new int[size]();

			if (type == 0) { // Scatter
				for(int i = 0; i < size; i++)
					data[i] = i;
			}
		}

		if (type == 0) { // Scatter
			int p = size / hg.size();
			int r = size % hg.size();

			size_t recvsize = p;
			if (r && rank < r) recvsize++;

			int *recvbuff = new int[recvsize]();
		
			if (hg.sendrecv(data, size * sizeof(int), recvbuff, recvsize * sizeof(int), sizeof(int)) <= 0) {
				MTCL_ERROR("[scatter_gather_test]:\t", "sendrecv failed\n");
				return -1;
			}

			hg.close();

			// just for having a "clean" printing to the stdout
			usleep(rank * 500000);

			std::cout << "rank:" << rank << " -> [";

			for(size_t i = 0; i < recvsize; i++) {
				std::cout << recvbuff[i];
				if (i != recvsize - 1) std::cout << ", ";
			}

			delete [] recvbuff;

			std::cout << "]";

			if (rank == node_root) {
				std::cout << " data_send -> [";

				for(int i = 0; i < size; i++) {
					std::cout << data[i];
					if (i != size - 1) std::cout << ", ";
				}

				std::cout << "]";

				delete [] data;
			}

			std::cout << "\n";
		} else { // Gather
			int p = size / hg.size();
			int r = size % hg.size();

			size_t sendsize = p;
			if (r && rank < r) sendsize++;

			int *sendbuff = new int[sendsize]();

			for(size_t i = 0; i < sendsize; i++)
				sendbuff[i] = rank;
		
			if (hg.sendrecv(sendbuff, sendsize * sizeof(int), data, size * sizeof(int), sizeof(int)) <= 0) {
				MTCL_ERROR("[scatter_gather_test]:\t", "sendrecv failed\n");
				return -1;
			}

			hg.close();

			usleep(rank * 500000);

			std::cout << "rank:" << rank << " -> [";

			for(size_t i = 0; i < sendsize; i++) {
				std::cout << sendbuff[i];
				if (i != sendsize - 1) std::cout << ", ";
			}

			delete [] sendbuff;

			std::cout << "]";

			if (rank == node_root) {
				std::cout << " data_receive -> [";

				for(int i = 0; i < size; i++) {
					std::cout << data[i];
					if (i != size - 1) std::cout << ", ";
				}

				std::cout << "]";

				delete [] data;
			}

			std::cout << "\n";
		}
	} else {
		usleep(rank * 500000);
		std::cout << "rank:" << rank << " -> ------" << std::endl;
	}
	
    Manager::finalize(true); 

    return 0;
}