#define SINGLE_IO_THREAD
#define MTCL_DISABLE_NAGLE
#define MTCL_DISABLE_COLLECTIVES
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

    int tasks = 1000;
    if (argc > 1)
        tasks = atoi(argv[1]);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0){
        Manager::connect("MPI:1");

        std::cout << "0: get an handle!\n";
        auto h = Manager::getNext();
        RequestPool rp(2);
        for (int counter=0; counter < tasks; counter++){
            char buff1[36]; 
            char buff2[11];
            h.ireceive(buff1, 35, rp);
            h.ireceive(buff2, 10, rp);
            buff1[35] = '\0';
            buff2[10] = '\0';
            rp.waitAll(); rp.reset();
            std::cout << "Str1: " << buff1 << std::endl;
            std::cout << "Str2: " << buff2 << std::endl;
	    }
        h.close();

    } else {
        MTCL::RequestPool pool(2);
        auto h = Manager::getNext();
        if (h.isNewConnection()) {
            std::cout << "1: Received new connection!\n";

            for(int pippo=0; pippo < tasks; pippo++){
                std::string* payload = new std::string("THIS IS THE PAYLOAD OF THE MESSAGE!");
                
                h.isend(payload->c_str(), 35, pool);

                std::string* payload2 = new std::string(10, 'P');
                h.isend(payload2->c_str(), 10, pool);
                
                pool.waitAll();
                pool.reset();

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
