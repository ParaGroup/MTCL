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

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if(rank == 0) {
        {
            auto handle = Manager::connect("MPIP2P:");
            if(handle.isValid()) {
                char buff[5]{'p','i','n','g','\0'};
                ssize_t size = 5;

                printf("Trying to read\n");
                ssize_t count = 0;
                if((count = handle.read(buff, size)) == 0) {
                // if((count = handle.send(buff, size)) == 0) {
                    printf("Server dropped connection\n");
                    handle.close();
                }                
            }
            // implicit handle.yield() when going out of scope
        }
        Manager::endM();
    }

    return 0;
}