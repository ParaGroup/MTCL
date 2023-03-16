#include <iostream>
#include <string>
#include <optional>
#include <thread>


#include <manager.hpp>
#include <protocols/mqtt.hpp>


int main(int argc, char** argv){
    
    if(argc < 2) {
        printf("Usage: %s <id> [queue_id]", argv[0]);
    }

    // Manager m;
    Manager::registerType<ConnMQTT>("MQTT");

    int id = atoi(argv[1]);

    if(id == 0) {
        Manager::listen("MQTT:0");
        Manager::init(argc, argv);
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
                    if(handle.read(buf, size) == 0) {
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
        Manager::init(argc, argv);
        char* queue_id = argv[2];

        // Qui la stringa è formattata come MQTT:manager_remoto:nome_coda
        // dove nome_coda è quella che verrà usata per tutte le comunicazioni
        // sulla connessione logica creata dalla connect
        auto handle = Manager::connect(std::string("MQTT:0:").append(queue_id));

        std::string message("Sending something to ");
        message.append(queue_id);

        handle.send(message.c_str(), message.length());
        char buff[100];
        ssize_t size = 100;
        handle.read(buff, size);
        printf("Read from server: %s\n", buff);

        if(handle.read(buff, size) == 0) {
            printf("Correctly closed connection\n");
        }

    }
    Manager::endM();

    return 0;
}