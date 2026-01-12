#include <iostream>
#include "mtcl.hpp"
using namespace MTCL;

int main(int argc, char** argv){
#ifdef EXCLUDE_MPI
    std::cerr << "You must compile with MPI support this test\n";
    return 1;
#endif
	int rc=0;
    Manager::init("");

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0){
        auto h = Manager::connect("MPI:1:2");
        std::cout << "0: handle received!\n";
        while(true){
            char tmp;
            if (h.receive(&tmp, 1) == 0){
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
            h.close();
            std::cout << "1: connection closed!\n";
            // should be an error here since we send after closing connection!
            if (h.send("a", 1) < 0)
                std::cerr << "1: Expected error in sending!\n";
            else {
                std::cout << "1: sended a\n";
				rc = -1;
			}
        }
    }

    Manager::finalize(true);
    return rc;

}
