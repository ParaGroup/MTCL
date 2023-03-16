/*
 *
 * Simple example using both group and p2p handles. Broadcast and gather patterns
 * are used to connect Workers to Emitter and Collector, respectively. Collector
 * is also connected to the Emitter via a p2p handle, emulating a feedback channel.
 * 
 *       ___________________________P2P handle_____________________________________
 *      |                                                                          |
 *      v                    | -> Worker (App2) -> |                               |
 * Emitter (App1) --BCast--> |                     | --Gather--> Collector (App4) --
 *                           | -> Worker (App3) -> |                         
 *      
 *
 * Compile with:
 *  $> RAPIDJSON_HOME="/rapidjson/install/path" make clean test_farm_bcast
 * 
 * Execution (with the same order as below):
 *  $> ./test_farm_bcast 0 App1 <stream_len>
 *  $> ./test_farm_bcast 3 App4 <stream_len>
 *  $> ./test_farm_bcast 1 App2 <stream_len>
 *  $> ./test_farm_bcast 2 App3 <stream_len>
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
    int* data = new int[streamlen];

    // Emitter
    if(rank == 0) {
        auto fbk = Manager::getNext();
        
        // Sending stream len to collector
        printf("Sending streamlen (%d) to collector\n", streamlen);
        fbk.send(&streamlen, sizeof(int));
        fbk.setName("Collector");
        fbk.yield();

        auto hg = Manager::createTeam("App1:App2:App3", "App1", BROADCAST);
        if(hg.isValid() && fbk.isValid())
            printf("Emitter starting\n");
        else
            printf("Aborting, connection error (group: %d - p2p: %d)\n", hg.isValid(), fbk.isValid());


        // Sending buffer of data to each participant
        for(int i = 0; i < streamlen; i++) {
            data[i] = i+1;
        }
        if(hg.sendrecv(data, streamlen*sizeof(int), nullptr, 0) <= 0) {
            printf("Error sending message\n");
            return 1;
        }
        hg.close();

        int res = 0;
        ssize_t r;
        do {
            auto h = Manager::getNext();
            auto name = h.getName();
            r = h.receive(&res, sizeof(int));
            if(r > 0)
                printf("Received update from Collector. Current value is: %d\n", res);
        }while(r > 0);
        fbk.close();

        printf("Total is: %d, expected was: %d\n", res, expected);

    }
    // Collector
    else if(rank==3) {
        // Feedback channel
        auto fbk = Manager::connect("TCP:0.0.0.0:42000");

        int stream_len;
        fbk.receive(&stream_len, sizeof(int));
        printf("Stream len is %d\n", streamlen);

        auto hg_gather = Manager::createTeam("App2:App3:App4", "App4", GATHER);

        int partial = 0;
        int gather_data[3];

        ssize_t r;
        do {
            r = hg_gather.sendrecv(&rank, sizeof(int), gather_data, sizeof(int));
            if(r == 0) {
                printf("gather closed\n");
                break;
            }
        } while(r > 0);
        hg_gather.close();

        partial += gather_data[0];
        partial += gather_data[1];
        printf("Collector computed %d\n", partial);

        fbk.send(&partial, sizeof(int));
        fbk.close();
    }
    // Worker
    else {
        auto hg_bcast = Manager::createTeam("App1:App2:App3", "App1", BROADCAST);
        auto hg_gather = Manager::createTeam("App2:App3:App4", "App4", GATHER);
        if(hg_bcast.isValid() && hg_gather.isValid())
            printf("Correctly created teams\n");
        else {
            printf("bcast: %d - gather: %d\n", hg_bcast.isValid(), hg_gather.isValid());
            return 1;
        }

        ssize_t res = hg_bcast.sendrecv(nullptr, 0, data, streamlen*sizeof(int));
        if(res <= 0) {
            printf("bcast error\n");        
            return 1;
        }

        int partial{0};
        for(int i = rank - 1; i < streamlen; i+=2) {
            partial += data[i];
            printf("Received el: %d - partial is: %d\n", data[i], partial);
        }
        hg_bcast.close();
        
        hg_gather.sendrecv(&partial, sizeof(int), nullptr, 0);
        hg_gather.close();
    }

    delete[] data;
    Manager::finalize();

    return 0;
}
