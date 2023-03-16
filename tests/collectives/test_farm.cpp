/*
 *
 * Simple example using both group and p2p handles. Fan-out and Fan-in patterns
 * are used to connect Workers to Emitter and Collector, respectively. Collector
 * is also connected to the Emitter via a p2p handle, emulating a feedback channel.
 * 
 *       ___________________________P2P handle_____________________________________
 *      |                                                                          |
 *      v                     | -> Worker (App2) -> |                              |
 * Emitter (App1) --FanOUT--> |                     | --FanIN--> Collector (App4) --
 *                            | -> Worker (App3) -> |                         
 *      
 *
 * Compile with:
 *  $> RAPIDJSON_HOME="/rapidjson/install/path" make clean test_farm
 * 
 * Execution (with the same order as below):
 *  $> ./test_farm 0 App1 <stream_len>
 *  $> ./test_farm 2 App4 <stream_len>
 *  $> ./test_farm 1 App2 <stream_len>
 *  $> ./test_farm 1 App3 <stream_len>
 * 
 * 
 * */



#include <iostream>
#include "mtcl.hpp"

inline static std::string hello{"Hello team!"};
inline static std::string bye{"Bye team!"};

int main(int argc, char** argv){

    if(argc < 4) {
        printf("Usage: %s <0|1|2> <App1|App2|App3|App4> <stream len>\n", argv[0]);
        return 1;
    }


    int rank = atoi(argv[1]);
    int streamlen = atoi(argv[3]);
	Manager::init(argv[2], "test_farm.json");

    int expected = (streamlen+1)*streamlen/2;

    // Emitter
    if(rank == 0) {
        auto fbk = Manager::getNext();
        
        // Sending stream len to collector
        printf("Sending streamlen (%d) to collector\n", streamlen);
        fbk.send(&streamlen, sizeof(int));
        fbk.setName("Collector");
        fbk.yield();

        auto hg = Manager::createTeam("App1:App2:App3", "App1", FANOUT);
        if(hg.isValid() && fbk.isValid())
            printf("Emitter starting\n");
        else
            printf("Aborting, connection error (group: %d - p2p: %d)\n", hg.isValid(), fbk.isValid());

        // Sending stream elements to Workers in the group
        for(int i = 1; i <= streamlen; i++) {
            if(hg.send(&i, sizeof(int)) <= 0)
                printf("Error sending message\n");
        }
        hg.close();

        int res = 0;
        ssize_t r;
        do {
            auto h = Manager::getNext();
            auto name = h.getName();
            r = h.receive(&res, sizeof(int));
            if(r!=0)
                printf("Received update from Collector. Current value is: %d\n", res);
        }while(r > 0);
        fbk.close();

        printf("Total is: %d, expected was: %d\n", res, expected);

    }
    // Worker
    else if(rank == 1){
        auto hg_fanout = Manager::createTeam("App1:App2:App3", "App1", FANOUT);
        auto hg_fanin = Manager::createTeam("App2:App3:App4", "App4", FANIN);
        if(hg_fanout.isValid() && hg_fanin.isValid())
            printf("Correctly created teams\n");

        int partial{0};
        ssize_t res = 0;

        do {
            int el = 0;
            res = hg_fanout.receive(&el, sizeof(int));    
            if(res <= 0) {
                printf("fanout closed\n");        
                break;
            }
            partial += el;
            printf("Received el: %d - partial is: %d\n", el, partial);
        } while(res > 0);
        hg_fanout.close();

        hg_fanin.send(&partial, sizeof(int));
        hg_fanin.close();
    }
    // Collector
    else {
        auto fbk = Manager::connect("TCP:0.0.0.0:42000");

        int stream_len;
        fbk.receive(&stream_len, sizeof(int));
        printf("Stream len is %d\n", streamlen);

        auto hg_fanin = Manager::createTeam("App2:App3:App4", "App4", FANIN);
        hg_fanin.yield();

        int partial = 0;

        ssize_t r;
        do {
            int el = 0;
            auto hg = Manager::getNext();
            r = hg.receive(&el, sizeof(int));
            if(r <= 0) {
                printf("fanin closed\n");
                break;
            }
            printf("Received %d\n", el);
            partial += el;
            fbk.send(&partial, sizeof(int));
        } while(r > 0);

        printf("Collector computed %d\n", partial);
        hg_fanin.close();
        fbk.close();
    }

    Manager::finalize();

    return 0;
}
