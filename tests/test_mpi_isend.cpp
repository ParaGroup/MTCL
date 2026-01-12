#define SINGLE_IO_THREAD
#define MTCL_DISABLE_NAGLE
#define MTCL_DISABLE_COLLECTIVES
#include <iostream>
#include "mtcl.hpp"

using namespace MTCL;

int main(int argc, char** argv){
#ifdef EXCLUDE_MPI
    std::cerr << "You must compile with MPI support this test\n";
    return 1;
#endif

    Manager::init("");

    int tasks = 1000;
    if (argc > 1)
        tasks = atoi(argv[1]);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0){
        Manager::connect("MPI:1");

        std::cout << "0: get an handle!\n";
        auto h = Manager::getNext();
        for (int counter=1; counter < tasks*2; counter++){
	        size_t sz;
            if (h.probe(sz, true) == 0){
                h.close();
                break;
            };
            char buff[sz+1];
            h.receive(buff, sz);
            buff[sz] = '\0';
            std::cout << "Received a string of size: " << sz << std::endl;
	    }

    } else {
        auto h = Manager::getNext();
        if (h.isNewConnection()) {
            std::cout << "1: Received new connection!\n";

            for(int pippo=1; pippo < tasks; pippo++){
                std::string* payload = new std::string( "THIS IS THE PAYLOAD OF THE MESSAGE!" + std::to_string(pippo));
                
                MTCL::Request req;
                h.isend(payload->c_str(), payload->size(), req);


                MTCL::Request req2;
                std::string* payload2 = new std::string(pippo, 'P');
                h.isend(payload2->c_str(), payload2->size(), req2);
                
                MTCL:: waitAll(req, req2);

                delete payload;
                delete payload2;
            }

            h.close();
            std::cout << "1: connection closed!\n";
        }
    }


    Manager::finalize();
    return 0;

}
