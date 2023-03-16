/*
 *
 * Fan-in implementation test
 *
 *
 * Compile with:
 *  $> RAPIDJSON_HOME="/rapidjson/install/path" make clean test_fanin
 * 
 * Execution:
 *  $> ./test_fanin 0 App1
 *  $> ./test_fanin 1 App2
 *  $> ./test_fanin 1 App3
 * 
 * 
 * 
 * */



#include <iostream>
#include "mtcl.hpp"

inline static std::string hello{"Hello team!"};
inline static std::string bye{"Bye team!"};

int main(int argc, char** argv){

    if(argc < 2) {
        printf("Usage: %s <0|1> <Server|Client1|Client2>\n", argv[0]);
        return 1;
    }

    std::string listen_str{};
    std::string connect_str{};

    int rank = atoi(argv[1]);
	Manager::init(argv[2], "test_collectives.json");

    // Root
    if(rank == 0) {
        auto hg = Manager::createTeam("App1:App2:App3", "App1", FANIN);

        if(hg.isValid())
            printf("Correctly created team\n");

        char* s = new char[hello.length()+1];
        if (hg.receive(s, hello.length()) != (ssize_t)hello.length()) {
			abort();
		}
        s[hello.length()] = '\0';
        std::cout << "Received: " << s << std::endl;
        delete[] s;
		
        s = new char[bye.length()+1];
        if (hg.receive(s, bye.length()) != (ssize_t)bye.length()) {
			abort();
		}
        s[bye.length()] = '\0';
        printf("Received bye: %s\n", s);
        delete[] s;
        hg.close();


#if 1
		// anche cosi' bomba perche' la finalize rifa' la probe e legge quello che c'e' dopo
		// questo e' comunque un errore, la finalize dovrebbe accorgersi che la probe e' stata
		// gia' fatta
		size_t sz = 0;
		if (hg.probe(sz) == -1) {
			fprintf(stderr, "ERROR IN PROBE\n");
			abort();
		}
		fprintf(stderr, "PROBED SIZE= %ld\n", sz);
#endif
		
    }
    else {
        auto hg = Manager::createTeam("App1:App2:App3", "App1", FANIN);

        if(hg.isValid()) {
            printf("Correctly created team\n");
			if(std::string{argv[2]} == "App2") {
				hg.send((void*)hello.c_str(), hello.length());
				//hg.close();
			}
			if(std::string{argv[2]} == "App3") {
				if (hg.send((void*)bye.c_str(), bye.length()) != (ssize_t)bye.length()) {
					fprintf(stderr, "ERRORE1\n");
					abort();
				}
				//sleep(1);
				if (hg.send((void*)bye.c_str(), bye.length()) != (ssize_t)bye.length()) {
					fprintf(stderr, "ERRORE\n");
					abort();
				}
				hg.close();
			}					   			
		}
    }

    Manager::finalize(true);

    return 0;
}
