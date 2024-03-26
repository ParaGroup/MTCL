#include <iostream>
#include "mtcl.hpp"
#include "async.hpp"
using namespace MTCL;
int main(int argc, char** argv){
#ifdef EXCLUDE_MPI
    std::cerr << "You must compile with MPI support this test\n";
    return 1;
#endif

    Manager::init("");

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0){
        Manager::connect("MPI:1");
        while(true){
            std::cout << "0: get an handle!\n";
            auto h = Manager::getNext();
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

        {
            std::string payload = "THIS IS THE PAYLOAD OF THE MESSAGE!";
            MTCL::Request req;
            h.isend(payload.c_str(), payload.size(), req);


            MTCL::Request req2;
                std::string payload2(8000000, 'P');
                h.isend(payload2.c_str(), payload2.size(), req2);
            waitAll(req, req2);
        }

            h.close();
            std::cout << "1: connection closed!\n";
        }
    }


    Manager::finalize();
    return 0;

}