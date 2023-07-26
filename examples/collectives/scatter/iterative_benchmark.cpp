/*
 *
 * Iterative benchmark testing MTCL collectives implementation. 
 * This test is focused on the Scatter and Gather collectives. 
 * The former allows the Emitter to send data to each Worker, 
 * the latter allows the Collector to retrieve partial results. 
 * The final result is then shipped via a P2P feedback channel
 * by the Collector to the Emitter. Each communication channel 
 * is tied to the enabled protocol at compilation time.
 * 
 * The configuration file, if not provided, is automatically 
 * generated by the application itself upon execution of the 
 * Emitter node to run on a single machine.
 * 
 * The application structure is as follows:
 * 
 *    ______________________P2P handle_________________________
 *   |                                                         |
 *   v            | --> Worker1 (App2) --> |                   |
 * Emitter -----> |        ...             | ----> Collector --
 *                | --> WorkerN (App3) --> |                         
 *          scatter                           gather  
 *
 * Compile with:
 *  $> TPROTOCOL=TCP||UCX||MPI [UCX_HOME="<ucx_path>" UCC_HOME="<ucc_path>"] RAPIDJSON_HOME="<rapidjson_path>" make clean iterative_benchmark
 * 
 * Execute with:
 *  $> ./iterative_benchmark <id> <num_workers> <iterations> <initial_size> [configuration_file_path]
 * 
 * Parameters are:
 *   - id: specify which part of the application to run.
 *          (Emitter id: 0, Collector id: 1, Worker id: [2,num_workers+1))
 *   - num_workers: specify how many workers need to be launched
 *   - iterations : total number of iterations
 *   - initial_size: is the size used in the first iteration, then the size is (initial_size * 2^(iter_index-1))
 *
 * Execution example with 2 workers, 10 iterations and an initial size of 64 bytes
 *  $> ./iterative_benchmark 0 2 10 64
 *  $> ./iterative_benchmark 1 2 10 64
 *  $> ./iterative_benchmark 2 2 10 64
 *  $> ./iterative_benchmark 3 2 10 64
 * 
 * or using mpirun and the MPMD model:
 *  $> mpirun -x MTCL_VERBOSE="all" -n 1 ./iterative_benchmark 0 2 10 32 iterative_benchmark.json : \
 *            -x MTCL_VERBOSE="all" -n 1 ./iterative_benchmark 1 2 10 32 iterative_benchmark.json : \
 *            -x MTCL_VERBOSE="all" -n 1 ./iterative_benchmark 2 2 10 32 iterative_benchmark.json : \
 *            -x MTCL_VERBOSE="all" -n 1 ./iterative_benchmark 3 2 10 32 iterative_benchmark.json
 * 
 * 
 */

#include <fstream>
#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>

#include <iostream>
#include "mtcl.hpp"

static int EMITTER_RANK{0};
static int WORKER_RANK{2};
static int COLLECTOR_RANK{1};

inline static std::map<int, std::string> participants {
    {0,"Emitter"},
    {1,"Collector"},
    {2,"Worker"}
};

/**
 * @brief Generates configuration file required for this application with the
 * specified number of workers \b num_workers. Based on the enabled protocol,
 * the correct endpoint and protocol strings are generated automatically.
 * 
 * @param num_workers total number of workers
 */
void generate_configuration(int num_workers) {
    std::string PROTOCOL{};
    std::string EMITTER_ENDPOINT{};
    std::string COLLECTOR_ENDPOINT{};

#ifdef ENABLE_TCP
    PROTOCOL = {"TCP"};
    EMITTER_ENDPOINT = {"TCP:0.0.0.0:42000"};
    COLLECTOR_ENDPOINT = {"TCP:0.0.0.0:42001"};
#endif

#ifdef ENABLE_MPI
    PROTOCOL = {"MPI"};
    EMITTER_ENDPOINT = {"MPI:0:10"};
    COLLECTOR_ENDPOINT = {"MPI:1:10"};
#endif

#ifdef ENABLE_UCX
    PROTOCOL = {"UCX"};
    EMITTER_ENDPOINT = {"UCX:0.0.0.0:42000"};
    COLLECTOR_ENDPOINT = {"UCX:0.0.0.0:42001"};
#endif

    rapidjson::Value s;
    rapidjson::Document doc;
    doc.SetObject();
    rapidjson::Value components;
    components.SetArray();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();
    
    rapidjson::Value protocols;
    protocols.SetArray();
    s.SetString(PROTOCOL.c_str(), PROTOCOL.length(), allocator); 
    protocols.PushBack(s, allocator);

    rapidjson::Value listen_endp_em;
    listen_endp_em.SetArray();
    s.SetString(EMITTER_ENDPOINT.c_str(), EMITTER_ENDPOINT.length(), allocator); 
    listen_endp_em.PushBack(s, allocator);
    rapidjson::Value listen_endp_coll;
    listen_endp_coll.SetArray();
    s.SetString(COLLECTOR_ENDPOINT.c_str(), COLLECTOR_ENDPOINT.length(), allocator); 
    listen_endp_coll.PushBack(s, allocator);
    
    
    rapidjson::Value emitter;
    emitter.SetObject();
    emitter.AddMember("name", "Emitter", allocator);
    emitter.AddMember("host", "localhost", allocator);
    emitter.AddMember("protocols", protocols, allocator);
    emitter.AddMember("listen-endpoints", listen_endp_em, allocator);

    rapidjson::Value collector;
    collector.SetObject();
    collector.AddMember("name", "Collector", allocator);
    collector.AddMember("host", "localhost", allocator);
    protocols.SetArray();
    s.SetString(PROTOCOL.c_str(), PROTOCOL.length(), allocator); 
    protocols.PushBack(s, allocator);
    collector.AddMember("protocols", protocols, allocator);
    collector.AddMember("listen-endpoints", listen_endp_coll, allocator);


    components.PushBack(emitter, allocator);

    for(int i=1; i<=num_workers; i++) {
        std::string worker_i{participants.at(WORKER_RANK) + std::to_string(i)};
        
        rapidjson::Value worker;
        worker.SetObject();
        worker.AddMember("host", "localhost", allocator);
        protocols.SetArray();
        s.SetString(PROTOCOL.c_str(), PROTOCOL.length(), allocator); 
        protocols.PushBack(s, allocator);
        worker.AddMember("protocols", protocols, allocator);
        s.SetString(worker_i.c_str(), worker_i.length(), allocator); 
        worker.AddMember("name", s, allocator);
        
        components.PushBack(worker, allocator);
    }
    components.PushBack(collector, allocator);

    doc.AddMember("components", components, allocator);
    std::ofstream ofs("iterative_bench_auto.json");
    rapidjson::OStreamWrapper osw(ofs);

    rapidjson::Writer<rapidjson::OStreamWrapper> writer(osw);
    doc.Accept(writer);
}

void Emitter(const std::string& scatter_participants, const std::string& scatter_root, const int iterations, size_t size) {

	// Waiting for Collector
	auto fbk = Manager::getNext();
	if (!fbk.isValid()) {
		MTCL_ERROR("[Emitter]:\t", "Manager::getNext, invalid feedback handle\n");
		return;
	}
	fbk.yield(); // give it back to the Manager
	
	auto hg = Manager::createTeam(scatter_participants, scatter_root, SCATTER);
	if (hg.isValid()) {
		MTCL_PRINT(0,"[Emitter]:\t", "Emitter starting\n");
	} else {
		MTCL_ERROR("[Emitter]:\t", "Manager::createTeam for SCATTER, ERROR\n");
		return;
	}

	for(int i = 0; i < iterations; i++) {
		MTCL_PRINT(0, "[Emitter]:\t", "starting iteration %d, size=%ld\n", i, size);
		
		int *data = new int[size];

		for (int i = 0, j = 100; i < size; i++, j += 100) {
			data[i] = j;
		}

		int recvsize = size / hg.size();
		int *recvbuf = new int[recvsize];

		if (hg.sendrecv(data, size * sizeof(int), recvbuf, recvsize * sizeof(int), sizeof(int)) <= 0) {
			MTCL_ERROR("[Emitter]:\t", "send from scatter ERROR\n");
			return;
		}

		std::cout << "SelfMessage-" << i << ": [ ";

		for (int i = 0; i < recvsize; i++) {
			std::cout << recvbuf[i] << " ";
		}
		
		std::cout << "]" << std::endl;	   			
		
		// Receive result from feedback channel
		auto h = Manager::getNext();
		int res;
		if (h.receive(&res, sizeof(int)) <= 0) {
			MTCL_ERROR("[Emitter]:\t", "receive from feedback ERROR\n");
			return;
		}
		if (res != COLLECTOR_RANK) {
			MTCL_ERROR("[Emitter]:\t", "receive from feedback, WRONG DATA\n");
			return;
		}
		
		delete [] data;
		size = size << 1;
		MTCL_PRINT(0, "[Emitter]:\t", "done iteration %d\n", i);

	}
	auto h = Manager::getNext();	// waiting for the close of the Collector
	h.close();
	hg.close();
}

void Worker(const std::string& scatter_participants, const std::string& gather,
			const std::string& scatter_root, const std::string& groot,
			const int rank, const int iterations, size_t size) {

	MTCL_PRINT(0, "[Worker]:\t", "scatter_participants=%s, gather=%s, broot=%s, groot=%s, Worker%d\n",
			   scatter_participants.c_str(), gather.c_str(), scatter_root.c_str(), groot.c_str(), rank);
		
	auto hg_scatter = Manager::createTeam(scatter_participants, scatter_root, SCATTER);	
	auto hg_gather= Manager::createTeam(gather, groot, MTCL_GATHER);
		
	if (hg_scatter.isValid() && hg_gather.isValid()) {
		MTCL_PRINT(0, "[Worker]:\t", "%s%d, starting\n", "Worker", rank);
	} else {
		MTCL_ERROR("[Worker]:\t", "Manager::createTeam, invalid collective handles (SCATTER and/or GATHER)\n");
		return;
	}
	
	for(int i=0; i< iterations; ++i) {
		MTCL_PRINT(0, "[Worker]:\t", "Worker%d, starting iteration %d, size=%ld\n", rank, i, size);
		int datasize = size / hg_scatter.size();
		int *data = new int[datasize];
		hg_scatter.sendrecv(nullptr, 0, data, datasize * sizeof(int));		   			
		hg_gather.sendrecv(data, datasize * sizeof(int), nullptr, 0);

		delete [] data;
		size = size << 1;
		MTCL_PRINT(0, "[Worker]:\t", "Worker%d, done iteration %d\n", rank, i);
	}
	
	hg_scatter.close();        
	hg_gather.close();	
}

void Collector(const std::string& gather, const std::string& groot,
			   const int nworkers, const int iterations, size_t size) {

	auto fbk = Manager::connect("Emitter", 100, 200);
	if(!fbk.isValid()) {
		MTCL_ERROR("[Collector]:\t", "Manager::connect, cannot connect to the Emitter\n");
		return;
	}

	auto hg = Manager::createTeam(gather, groot, MTCL_GATHER);
	if (hg.isValid()) {
		MTCL_PRINT(0,"[Collector]:\t", "Collector starting\n");
	} else {
		MTCL_ERROR("[Collector]:\t", "Manager::createTeam for GATHER, ERROR\n");
		return;
	}
	
	int *mydata;
	for(int i=0; i< iterations; ++i) {
		MTCL_PRINT(0, "[Collector]:\t", "starting iteration %d, size=%ld\n", i, size);
		int *gatherdata = new int[size]();
		int mydatasize = size / (nworkers + 1);
		mydata = new int[mydatasize]();
		
		hg.sendrecv(mydata, mydatasize * sizeof(int), gatherdata, mydatasize * sizeof(int));

		for (int i = 0; i < nworkers + 1; i++) {
			std::cout << "Message-" << i << ": [ ";

			for (int j = 0; j < mydatasize; j++) {
				std::cout << gatherdata[(i * mydatasize) + j] << " ";
			}

			std::cout << "]" << std::endl;
		} 

		if (fbk.send(&COLLECTOR_RANK, sizeof(int))<=0) {
			MTCL_ERROR("[Collector]:\t", "send to the Emitter ERROR\n");
			return;
		}
		
		delete [] gatherdata;
		delete [] mydata;
		
		size = size << 1;
		MTCL_PRINT(0, "[Collector]:\t", "done iteration %d\n", i);
	}
	fbk.close();		
	hg.close();
}


int main(int argc, char** argv){

    if(argc < 5) {
		std::cout << "Usage: " << argv[0] << " <0|1|2> <num workers> <n. iterations> <initial_size(elements)> [configuration_file]\n";
		std::cout << "      - 0 Emitter, 1 Collector, 2 for each Worker\n";
		std::cout << "      - initial_size for the first iteration, then the size is (initial_size * 2^(iter_index-1))\n";
		std::cout << "        (e.g., 5 iterations, 512 initial size  --> size= 512, 1024, 2048, 4096, 8196)\n";
        return 1;
    }

    int rank        = std::stol(argv[1]);
    int num_workers = std::stol(argv[2]);
    int iterations  = std::stol(argv[3]);
    size_t size     = std::stol(argv[4]);

    std::string configuration_file{"iterative_bench_auto.json"};
    // A user provided configuration file was specified
    if(argc == 6) {
        configuration_file = {argv[5]};
    } else {
        generate_configuration(num_workers);
    }

    // Generating appName based on AppID
    std::string appName{""};
    if(rank >= WORKER_RANK) {
        appName = participants.at(WORKER_RANK) + std::to_string(rank-1);
    } else appName = participants.at(rank);

	std::string scatter_string{participants.at(EMITTER_RANK)};
	std::string worker_string{};
    std::string gather_string{participants.at(COLLECTOR_RANK)};

    for(int i=1; i<=num_workers; i++) {
        std::string worker_i{participants.at(WORKER_RANK) + std::to_string(i)};
        worker_string += (":" + worker_i);
    }
    scatter_string += worker_string;
    gather_string += worker_string;

	if (Manager::init(appName, configuration_file) < 0) {
		MTCL_ERROR("[MTCL]:\t", "Manager::init ERROR\n");
		return -1;
	}

    if(rank == 0) {
		Emitter(scatter_string, participants.at(EMITTER_RANK), iterations, size);
    } else {
		if (rank == 1) {
			Collector(gather_string, participants.at(COLLECTOR_RANK), participants.size()-1,
					  iterations, size);
		} else {	   
			Worker(scatter_string, gather_string,
				   participants.at(EMITTER_RANK), participants.at(COLLECTOR_RANK),
				   rank-1, iterations, size);
		}
	}
	
    Manager::finalize(true); 
    return 0;
}
