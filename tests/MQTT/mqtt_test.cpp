#include <iostream>
#include <string>
#include <optional>
#include <thread>

#include "mtcl.hpp"

using namespace MTCL;
int main(int argc, char** argv){
    
    if(argc < 3) {
        printf("Usage: %s <id> queue_id", argv[0]);
    }

    // Manager m;
    Manager::registerType<ConnMQTT>("MQTT");

    int id = atoi(argv[1]);
	Manager::init(argv[1]);
    if(id == 0) {
        Manager::listen("MQTT:0");

        // Listening for new connections
        while(true) {
            auto handle = Manager::getNext();
            // auto handle = m.getReady();
            if(handle.isValid()) {
                if(handle.isNewConnection()) {
                    printf("Got new connection\n");
                }
                else {
                    char buf[100];
                    ssize_t size = 100;
                    if(handle.receive(buf, size) == 0) {
                        printf("Connection closed by peer\n");
                        break;
                    }

                    printf("Read from client: %s\n", buf);
                    handle.send(buf, size);
                    handle.close();
                }
            }
            else {
                printf("No value in handle\n");
            }
        }
    }
    else {
        Manager::listen("MQTT:1");
        char* queue_id = argv[2];

        auto handle = Manager::connect(std::string("MQTT:0:").append(queue_id));

        std::string message("Sending something to ");
        message.append(queue_id);

        handle.send(message.c_str(), message.length());
        char buff[100];
        ssize_t size = 100;
        handle.receive(buff, size);
        printf("Read from server: %s\n", buff);

        if(handle.receive(buff, size) == 0) {
            printf("Correctly closed connection\n");
        }

    }
    Manager::finalize();

    return 0;
}
