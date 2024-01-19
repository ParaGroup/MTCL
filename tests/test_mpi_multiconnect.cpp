#include <iostream>
#include "mtcl.hpp"

int main(int argc, char** argv){
#ifdef EXCLUDE_MPI
    std::cerr << "You must compile with MPI support this test\n";
    return 1;
#endif

    MTCL::Manager::init("e");

    int rank;
    sleep(5);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0){
        auto h1 = MTCL::Manager::connect("MPI:1");
        auto h2 = MTCL::Manager::connect("MPI:1");

        std::cout << "First connection id: " << h1.getID() << std::endl;
        std::cout << "Second connection id: " << h2.getID() << std::endl;
        sleep(1);
        int dummy = 1000;
        h1.send(&dummy, sizeof(dummy));
        sleep(1);
        h2.send(&dummy, sizeof(dummy));
        sleep(1);
        auto hr = MTCL::Manager::getNext();
        std::cout << "Received message from rank 0 of id: " << hr.getID() << std::endl;

    } else {
        int dummy;
        ///MTCL::Manager::listen("MPI:1:2");
        auto h = MTCL::Manager::getNext();
        if (h.isNewConnection()) {
            std::cout << "Receiver id: " << h.getID() << std::endl;
        }
        h.yield();
        h = MTCL::Manager::getNext();
        if (h.isNewConnection()) {
            std::cout << "Receiver id: " << h.getID() << std::endl;
        }
        h.yield();
        h = MTCL::Manager::getNext();
        std::cout << "Receiver id: " << h.getID() << std::endl;
        size_t sz;
        h.probe(sz);
        assert(sz == sizeof(dummy));
        h.receive(&dummy, sz);
        h.yield();
        h = MTCL::Manager::getNext();
        std::cout << "Receiver id: " << h.getID() << std::endl;
        h.probe(sz);
        assert(sz == sizeof(dummy));
        h.receive(&dummy, sz);

        auto hr = MTCL::Manager::connect("MPI:0:1");
        std::cout << "Backwards connect id: " << hr.getID() << std::endl; 

    }

    MTCL::Manager::finalize();
    return 0;

}