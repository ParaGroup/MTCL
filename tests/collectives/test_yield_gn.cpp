/*
 * Tests yield and getNext while participating, as a receiver, in multiple teams.
 * In this test, App3 is a participant in both a FanIN and a FanOUT collective.
 * In the former, App3 is the root of the collective, hence it receives data from
 * all the participants. In the latter, App3 is non-root, hence it receives data
 * from the root.
 * In this case, App3 could relinquish control of both collectives handle to the
 * Manager and wait for data to be ready to be read. * 
 * 
 * App3 is receiving data from more than one team:
 *  - FanIN  (App1:App2:App3 - root: App3)
 *  - FanOUT (App1:App2:App3 - root: App2)
 * 
 * Application structure:
 * ======================
 * 
 *            |-- FanOut --> [App1] -- FanIn --> |
 *            |                                  |
 * [App2] --> |------------- FanIn ------------> | --> [App3]
 *            |                                  |
 *            |------------- FanOut -----------> |
 * 
 * ======================
 * 
 * Compile with:
 *  $> RAPIDJSON_HOME="/rapidjson/install/path" make clean test_yield_gn
 * 
 * Execution (with the same order as below):
 *  $> ./test_yield_gn 2 App3
 *  $> ./test_yield_gn 1 App2
 *  $> ./test_yield_gn 0 App1
 * 
 * 
 * */



#include <iostream>
#include "mtcl.hpp"

inline static std::string hello{"Hello team!"};
inline static std::string bye{"Bye team!"};

int main(int argc, char** argv){

    if(argc < 3) {
        printf("Usage: %s <0|1|2> <App1|App2|App3>\n", argv[0]);
        return 1;
    }


    int rank = atoi(argv[1]);
	Manager::init(argv[2], "test_collectives.json");

    // App1
    if(rank == 0) {
        auto hg_fanin = Manager::createTeam("App1:App2:App3", "App3", FANIN);
        auto hg_fanout = Manager::createTeam("App1:App2:App3", "App2", FANOUT);
        
        if(!hg_fanin.isValid() || !hg_fanout.isValid()) {
            printf("Group creation failed\n");
            return 1;
        }
        
        hg_fanout.yield();

        hg_fanin.send(hello.c_str(), hello.length());
        hg_fanin.send(bye.c_str(), bye.length());
        hg_fanin.close();

        ssize_t res;
        while(true) {
            auto hg = Manager::getNext();
            if(hg.getType() != FANOUT) {
                printf("Expected FANOUT [%d] type, got: %d\n", FANOUT, hg.getType());
                break;
            }

            char* s = new char[hello.length()+1];
            if((res = hg.receive(s, hello.length())) <= 0) {
                printf("Received res: %ld - closing.\n", res);
                hg.close();
                break;
            }
            s[hello.length()] = '\0';
            printf("Received: %s\n", s);
        }

    }
    // App2
    else if(rank == 1){
        auto hg_fanin = Manager::createTeam("App1:App2:App3", "App3", FANIN);
        auto hg_fanout = Manager::createTeam("App1:App2:App3", "App2", FANOUT);

        if(!hg_fanin.isValid() || !hg_fanout.isValid()) {
            printf("Group creation failed\n");
            return 1;
        }

        hg_fanin.send(hello.c_str(), hello.length());
        hg_fanout.send(bye.c_str(), bye.length());
        hg_fanin.send(bye.c_str(), bye.length());
        hg_fanout.send(bye.c_str(), bye.length());
        hg_fanin.close();
        hg_fanout.close();

        
    }
    // App3
    else {
        auto hg_fanin = Manager::createTeam("App1:App2:App3", "App3", FANIN);
        auto hg_fanout = Manager::createTeam("App1:App2:App3", "App2", FANOUT);

        if(!hg_fanin.isValid() || !hg_fanout.isValid()) {
            printf("Group creation failed\n");
            return 1;
        }

        hg_fanin.yield();
        hg_fanout.yield();

        int count = 0;
        while(count < 2) {
            auto hg = Manager::getNext();
            size_t sz;
            if(hg.probe(sz, true) <= 0) {
                if(sz == 0) printf("Received EOS from %d\n", hg.getType());
                count++;
                continue;
            }
            char* s = new char[sz+1];
            hg.receive(s, sz);
            s[sz] = '\0';

            printf("Received %s from %d\n", s, hg.getType());
        }
        
    }

    Manager::finalize();

    return 0;
}
