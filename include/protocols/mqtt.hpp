#ifndef MQTT_HPP
#define MQTT_HPP

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>

#include <sys/types.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <vector>
#include <queue>
#include <map>
#include <shared_mutex>
#include <thread>
#include <atomic>

#include "mqtt/client.h"

#include "../handle.hpp"
#include "../protocolInterface.hpp"
#include "../config.hpp"
#include "../utils.hpp"


class HandleMQTT : public Handle {

public:
    // bool closing = false;
    bool is_probed = false;
    mqtt::client* client;
    mqtt::string out_topic, in_topic;
    std::queue<std::pair<std::string, ssize_t>> messages;


    HandleMQTT(ConnType* parent, mqtt::client* client,
        mqtt::string out_topic, mqtt::string in_topic, bool busy=true) :
            Handle(parent), client(client),
            out_topic(out_topic), in_topic(in_topic) {}

    ssize_t sendEOS() {
        size_t sz = 0;
        client->publish(out_topic, &sz, sizeof(size_t));
        return sizeof(size_t);
    }

    ssize_t send(const void* buff, size_t size) {
        // if(closing) {
        //     MTCL_MQTT_PRINT(100, "HandleMQTT::send connection ERROR\n");
        //     errno = ECONNRESET;
        //     return -1;
        // }
        size_t sz = htobe64(size);
        client->publish(out_topic, &sz, sizeof(size_t));
        client->publish(out_topic, buff, size);
        return size;
    }

    ssize_t receive(void* buff, size_t size){
        is_probed = false;
        while(true) {
            if(!messages.empty()) {
                auto msg = messages.front();
                memcpy(buff, msg.first.c_str(), msg.second);
                messages.pop();
                return msg.second;
            }

            mqtt::const_message_ptr msg;
            bool res = client->try_consume_message(&msg);

            // A message has been received
            if(res) {
                // This topic should have been subscribed upon creation of the
                // handle object
                if(msg->get_topic() == in_topic) {
                    memcpy(buff, msg->get_payload().c_str(), size);
                    return size;
                }
            }

            // if(closing) {
            //     return 0;
            // }
			if constexpr (MQTT_POLL_TIMEOUT) 
				std::this_thread::sleep_for(std::chrono::milliseconds(MQTT_POLL_TIMEOUT));
        }        
        return 0;
    }

    ssize_t probe(size_t& size, const bool blocking=true) {
        size_t sz;

        // It means the Manager put the message header in the queue for us
        /*NOTE: here we assume the handleUser interface does not allow the probe
                to be called twice if data are not read.
                Without this assumption, this implementation IS NOT correct*/
        if(is_probed) {
            auto msg = messages.front();
            memcpy(&sz, msg.first.c_str(), sizeof(size_t));
            messages.pop();
            size = be64toh(sz);
            return sizeof(size_t);
        }

        mqtt::const_message_ptr msg;
        if(blocking) {
            receive(&sz, sizeof(size_t));
        }
        else {
			// check for a message, if there is a message it is read
            bool res = client->try_consume_message(&msg);
            if(!res) {
                errno = EWOULDBLOCK;
                return -1;
            }

            memcpy(&sz, msg->to_string().c_str(), sizeof(size_t));
        }
    
        size = be64toh(sz);
        is_probed = true;
        return sizeof(size_t);
    }

    bool peek() {
        size_t sz;
        ssize_t res = this->probe(sz, false);
        return res > 0;
    }

    ~HandleMQTT() {
        delete client;
    }

};


class ConnMQTT : public ConnType {
private:
    std::string manager_name, new_connection_topic;
    std::string appName;
    size_t count = 0;

    void createClient(mqtt::string topic, mqtt::client *aux_cli) {
        auto aux_connOpts = mqtt::connect_options_builder()
            .user_name(topic)
            .password("passwd")
            .keep_alive_interval(std::chrono::seconds(30))
            .automatic_reconnect(std::chrono::seconds(2), std::chrono::seconds(30))
            .clean_session(false)
            .finalize();

        mqtt::connect_response rsp = aux_cli->connect(aux_connOpts);

        // "listening" on topic-in
        if (!rsp.is_session_present()) {
            aux_cli->subscribe({topic/*, topic+MQTT_EXIT_TOPIC*/}, {0/*, 0*/});
        }
        else {
			MTCL_MQTT_PRINT(100, "ConnMQTT::createClient: session already present. Skipping subscribe.\n");
        }
    }

protected:
    
    mqtt::client *newConnClient;

    std::atomic<bool> finalized = false;
    bool listening = false;
    
    std::map<HandleMQTT*, bool> connections;  // Active connections for this Connector
    REMOVE_CODE_IF(std::shared_mutex shm);

public:

   ConnMQTT(){};
   ~ConnMQTT(){};

    int init(std::string s) {
        appName = s + ":";
		std::string server_address{MQTT_SERVER_ADDRESS};
		char *addr;
		if ((addr=getenv("MQTT_SERVER_ADDRESS")) != NULL) {
			server_address = std::string(addr);
		}
        newConnClient = new mqtt::client(server_address, appName);
        auto connOpts = mqtt::connect_options_builder()
            .keep_alive_interval(std::chrono::seconds(30))
            .automatic_reconnect(std::chrono::seconds(2), std::chrono::seconds(30))
            .clean_session(true)
            .finalize();

		try {
			mqtt::connect_response rsp = newConnClient->connect(connOpts);
			if(rsp.is_session_present()) {
				MTCL_MQTT_PRINT(100, "ConnMQTT::init: session already present. Use a different ID for the MQTT client\n");
				errno = ECOMM;
				return -1;
			}
		} catch(...) {
			MTCL_MQTT_PRINT(100, "ConnMQTT::init ERROR, cannot connect to server %s\n", server_address.c_str());
			errno = ECOMM;
			return -1;
		}		
        return 0;
    }

    int listen(std::string s) {
        manager_name = s.substr(s.find(":")+1, s.length());

        if(manager_name.empty()) {
            MTCL_MQTT_PRINT(100, "ConnMQTT::listen: server listen string must be defined\n");
            errno = EINVAL;
            return -1;
        }

        new_connection_topic = manager_name + MQTT_CONNECTION_TOPIC;

		try {
			newConnClient->subscribe({new_connection_topic}, {0});
		} catch(...) {
			MTCL_MQTT_PRINT(100, "ConnMQTT::listen ERROR, cannot subscribe %s\n", new_connection_topic.c_str());
			errno = ECOMM;
			return -1;
		}
        
        listening = true;
		MTCL_MQTT_PRINT(1, "listening on: %s ; connection topic: %s\n", s.c_str(), new_connection_topic.c_str());
        return 0;
    }

    void update() {
        REMOVE_CODE_IF(std::unique_lock ulock(shm, std::defer_lock));

        // Consume messages
        mqtt::const_message_ptr msg;
        bool res = newConnClient->try_consume_message(&msg);
        (void)res;

        if (msg) {
            // New message on new connection topic, we create new client and
            // connect it to the topic specified on the message + predefined suffix
            if(msg->get_topic() == new_connection_topic) {
                
                mqtt::client* aux_cli =
                    new mqtt::client(MQTT_SERVER_ADDRESS,
                        msg->to_string().append(MQTT_IN_SUFFIX));
                createClient(msg->to_string().append(MQTT_IN_SUFFIX), aux_cli);

                // Must send the ACK to the remote end otherwise I can miss some
                // messages
                auto pubmsg = mqtt::make_message(msg->to_string().append(MQTT_OUT_SUFFIX), "ack");
                pubmsg->set_qos(1);
                aux_cli->publish(pubmsg);

                HandleMQTT* handle = new HandleMQTT(this, aux_cli,
                    msg->to_string().append(MQTT_OUT_SUFFIX),
                    msg->to_string().append(MQTT_IN_SUFFIX));

                REMOVE_CODE_IF(ulock.lock());
                connections.insert({handle, false});
                addinQ(true, handle);
                REMOVE_CODE_IF(ulock.unlock());
            }
        }
        else {
			if (!newConnClient->is_connected()) {
				MTCL_MQTT_PRINT(100, "update: lost connection, waiting a while for reconnecting\n");
				std::this_thread::sleep_for(std::chrono::milliseconds(MQTT_CONNECT_TIMEOUT));
				if (!newConnClient->is_connected()) {
					MTCL_MQTT_PRINT(100, "ConnMQTT::update: no connection yet, keep going...\n");
					return;
				}
				MTCL_MQTT_PRINT(100, "ConnMQTT::update: re-established connection\n");
			}
		}
		
		REMOVE_CODE_IF(ulock.lock());
        for (auto &[handle, to_manage] : connections) {
            if(to_manage) {
                mqtt::const_message_ptr msg;
                bool res = handle->client->try_consume_message(&msg);
                if(res) {
                    handle->is_probed = true;
                    // if(msg->get_topic() == handle->out_topic+MQTT_EXIT_TOPIC) {
                        // handle->closing = true;
                    // }
                    // else {
                        handle->messages.push({msg->get_payload(), msg->get_payload().length()});
                    // }
					to_manage = false;
					// NOTE: called with ulock lock hold. Double lock if there is the IO-thread!
					addinQ(false, handle);
                }
            }
        }
		REMOVE_CODE_IF(ulock.unlock());        
    }


    // String for connection composed of manager_id:topic
    Handle* connect(const std::string& address, int retry, unsigned timeout) {
        std::string manager_id = address.substr(0, address.find(":"));

        if(manager_id.empty()) {
            MTCL_MQTT_PRINT(100, "ConnMQTT::connect: server connect string must be defined\n");
            errno = EINVAL;
            return nullptr;
        }

        std::string topic = appName + std::to_string(count++);
        mqtt::string topic_out = topic+MQTT_OUT_SUFFIX;
        mqtt::string topic_in = topic+MQTT_IN_SUFFIX;

        mqtt::client *client = new mqtt::client(MQTT_SERVER_ADDRESS, topic);
        mqtt::connect_options connOpts;
	    connOpts.set_keep_alive_interval(20);
        connOpts.set_clean_session(true);

        client->connect(connOpts);
        client->subscribe({topic_out/*, topic_out+MQTT_EXIT_TOPIC*/}, {0/*,0*/});
		
		auto pubmsg = mqtt::make_message(manager_id + MQTT_CONNECTION_TOPIC, topic);
		pubmsg->set_qos(1);
		client->publish(pubmsg);

        // Waiting for ack before proceeding
        auto msg = client->consume_message();

        // we write on topic + MQTT_IN_SUFFIX and we read from topic + MQTT_OUT_SUFFIX
        HandleMQTT* handle = new HandleMQTT(this, client, topic_in, topic_out, true);
		{
			REMOVE_CODE_IF(std::unique_lock lock(shm));
			connections[handle] = false;
        }
		MTCL_MQTT_PRINT(100, "connected to: %s\n", (manager_id+MQTT_CONNECTION_TOPIC).c_str());
        return handle;
    }


    void notify_close(Handle* h, bool close_wr=true, bool close_rd=true) {
        REMOVE_CODE_IF(std::unique_lock l(shm));
        HandleMQTT* handle = reinterpret_cast<HandleMQTT*>(h);

        if(close_rd)
            connections.erase(handle);

        // if (!handle->closing){
        //     std::string aux("");
        //     handle->client->publish(mqtt::make_message(handle->out_topic+MQTT_EXIT_TOPIC, aux));
        //     handle->client->disconnect();
        // }
        // connections.erase(reinterpret_cast<HandleMQTT*>(h));
    }


    void notify_yield(Handle* h) override {
        HandleMQTT* handle = reinterpret_cast<HandleMQTT*>(h);
        REMOVE_CODE_IF(std::unique_lock l(shm));
        // connections[reinterpret_cast<HandleMQTT*>(h)] = true;

        if(handle->is_probed) {
            addinQ(false, handle);
            return;
        }

        auto it = connections.find(handle);
        if(it == connections.end())
            MTCL_MQTT_ERROR("Couldn't yield handle\n");
        it->second = true;
    }

    void end(bool blockflag=false) {
        auto modified_connections = connections;
        for(auto& [handle, to_manage] : modified_connections)
            if(to_manage)
                setAsClosed(handle, blockflag);

        delete newConnClient;
    }

};

#endif
