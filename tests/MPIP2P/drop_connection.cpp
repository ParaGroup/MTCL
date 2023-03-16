#include <iostream>
#include <string>
#include <optional>
#include <thread>

#include <mpi.h>


#include "../../manager.hpp"
#include "../../protocols/mpip2p.hpp"


int main(int argc, char** argv){

    Manager::registerType<ConnMPIP2P>("MPIP2P");
    Manager::init(argc, argv);
    Manager::listen("MPIP2P");

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Listening for new connections
    if(rank == 0) {
        auto handle = Manager::getNext();

        if(handle.isValid()) {
            if(handle.isNewConnection()) {
                printf("Got new connection, dropping it\n");
                // std::this_thread::sleep_for(std::chrono::milliseconds(5000));
                handle.close();
            }
        }
        else {
            printf("No value in handle\n");
        }
        Manager::endM();
    }

    return 0;
}