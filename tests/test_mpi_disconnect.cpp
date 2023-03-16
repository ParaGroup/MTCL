#include <iostream>
#include "../commlib.hpp"

int main(int argc, char** argv){
#ifdef EXCLUDE_MPI
    std::cerr << "You must compile with MPI support this test\n";
    return 1;
#endif

    Manager::init();

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0){
        Manager::connect("MPI:1:2");
        while(true){
            std::cout << "0: get an handle!\n";
            auto h = Manager::getNext();
            std::cout << "0: handle received!\n";
            char tmp;
            if (h.read(&tmp, 1) == 0){
                std::cout << "0: Peer closed connection\n";
                h.close();
                break;
            } else {
                std::cout << "0: Received something! {" << tmp << "}\n";
            }
        }


    } else {
        auto h = Manager::getNext();
        if (h.isNewConnection()) {
            std::cout << "1: Received new connection!\n";
            h.send("a", 1);
            h.send("b", 1);
            h.send("c", 1);
            std::cout << "1: sended a\n";
            h.close();
            std::cout << "1: connection closed!\n";
        }
    }


    Manager::finalize();
    return 0;

}