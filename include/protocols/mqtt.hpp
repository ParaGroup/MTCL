#pragma once

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
#include <cstdint>
#include <chrono>
#include <algorithm>
#include <memory>
#include <type_traits>
#include <utility>
#include <errno.h>

#include "mqtt/async_client.h"

#include "../handle.hpp"
#include "../protocolInterface.hpp"
#include "../config.hpp"
#include "../utils.hpp"

namespace MTCL {

namespace mqtt_detail {

// token non-blocking completion check with compatibility across Paho versions.
template <typename Tok>
auto token_try_wait(const Tok& tok, int) -> decltype(tok->try_wait(), bool()) {
    return tok->try_wait();
}
template <typename Tok>
bool token_try_wait(const Tok& tok, long) {
    return tok->wait_for(std::chrono::milliseconds(0));
}
template <typename Tok>
bool token_try_wait(const Tok& tok) {
    return token_try_wait(tok, 0);
}

} // namespace mqtt_detail

class HandleMQTT;

class requestMQTTSend : public request_internal {
    mqtt::delivery_token_ptr tok;
    std::shared_ptr<std::string> payload; // keep payload alive until delivered
public:
	ssize_t nbytes = -1;
	requestMQTTSend(mqtt::delivery_token_ptr t, std::shared_ptr<std::string> p, ssize_t n)
        : tok(std::move(t)), payload(std::move(p)), nbytes(n) {}

	ssize_t count() const override { return nbytes; }
	
    int test(int& result) override {
        if (!tok) { errno = ECOMM; result = 0; return -1; }
        if (!mqtt_detail::token_try_wait(tok)) { result = 0; return 0; }
        result = 1;
        if (tok->get_return_code() != 0) { errno = ECOMM; return -1; }
        return 0;
    }

    int wait() override {
        if (!tok) { errno = ECOMM; return -1; }
        try { tok->wait(); } catch (...) { errno = ECOMM; return -1; }
        if (tok->get_return_code() != 0) { errno = ECOMM; return -1; }
        return 0;
    }

    int make_progress() override { int r; return test(r); }
};

class requestMQTTRecv : public request_internal {
    HandleMQTT* h;
    void* buff;
    size_t expected;
    bool completed = false;
	ssize_t got = -1;

public:
    requestMQTTRecv(HandleMQTT* handle, void* b, size_t exp)
        : h(handle), buff(b), expected(exp) {}

	ssize_t count() const override { return got; }
    int test(int& result) override;
    int wait() override;
    int make_progress() override { int r; return test(r); }
};

class ConnRequestVectorMQTTSend : public ConnRequestVector {
    struct Slot {
        mqtt::delivery_token_ptr tok;
        std::shared_ptr<std::string> payload;
    };

    std::vector<Slot> slots;
    size_t currSize = 0;

public:
    ConnRequestVectorMQTTSend(size_t sizeHint = 1) : slots(sizeHint) {}

    Slot* getNextSlot() {
        if (currSize >= slots.size()) slots.resize(slots.size() * 2 + 1);
        return &slots[currSize++];
    }

    bool testAll() override {
        for (size_t i = 0; i < currSize; ++i) {
            auto& s = slots[i];
            if (!s.tok) return false;
            if (!mqtt_detail::token_try_wait(s.tok)) return false;
        }
        return true;
    }

    void waitAll() override {
        for (size_t i = 0; i < currSize; ++i) {
            auto& s = slots[i];
            if (s.tok) {
                try { s.tok->wait(); } catch (...) {}
            }
        }
    }

    void reset() override { currSize = 0; }
};

class ConnRequestVectorMQTTRecv : public ConnRequestVector {
    struct Slot {
        requestMQTTRecv req;
        bool active = false;

        Slot() : req(nullptr, nullptr, 0), active(false) {}
        Slot(HandleMQTT* h, void* b, size_t exp) : req(h, b, exp), active(true) {}
    };

    std::vector<Slot> slots;
    size_t currSize = 0;

public:
    ConnRequestVectorMQTTRecv(size_t sizeHint = 1) : slots(sizeHint) {}

    Slot* getNextSlot(HandleMQTT* h, void* b, size_t exp) {
        if (currSize >= slots.size()) slots.resize(slots.size() * 2 + 1);
        slots[currSize] = Slot(h, b, exp);
        return &slots[currSize++];
    }

    bool testAll() override {
        for (size_t i = 0; i < currSize; ++i) {
            if (!slots[i].active) continue;
            int done = 0;
            if (slots[i].req.test(done) < 0) {
                slots[i].active = false;
                continue;
            }
            if (!done) return false;
            slots[i].active = false;
        }
        return true;
    }

    void waitAll() override {
        for (size_t i = 0; i < currSize; ++i) {
            if (slots[i].active) {
                (void)slots[i].req.wait();
                slots[i].active = false;
            }
        }
    }

    void reset() override { currSize = 0; }
};


class HandleMQTT : public Handle {
public:
    // bool closing = false;
    // bool is_probed = false;
    mqtt::async_client* client;
    mqtt::string out_topic, in_topic;
    std::queue<std::pair<std::string, ssize_t>> messages;
    static constexpr size_t HDR_SZ = sizeof(uint64_t);

    HandleMQTT(ConnType* parent, mqtt::async_client* client,
        mqtt::string out_topic, mqtt::string in_topic, bool busy=true) :
            Handle(parent), client(client),
            out_topic(out_topic), in_topic(in_topic) {}

    inline void clearProbed() { probed = {false, 0}; }

    // Drain/discard exactly one *payload* message of size 'payload_sz' from the internal queue.
    // Precondition: probe() has already consumed the framed MQTT packet and has queued the payload
    //              as {payload_bytes, payload_sz} in 'messages'.
    bool drainCurrentMessage(size_t payload_sz) {
        while (!messages.empty()) {
            auto msg = messages.front();
            messages.pop();

            // Ignore late out-of-band ACKs
            if ((size_t)msg.second < HDR_SZ && msg.first == "ack") {
                continue;
            }
            if ((size_t)msg.second != payload_sz) {
                MTCL_MQTT_PRINT(100, "HandleMQTT::drainCurrentMessage EPROTO, size mismatch (expected %ld got %ld)\n",
                                (long)payload_sz, (long)msg.second);
                errno = EPROTO;
                return false;
            }
            return true; // drained
        }
        MTCL_MQTT_PRINT(100, "HandleMQTT::drainCurrentMessage EPROTO, missing payload after probe\n");
        errno = EPROTO;
        return false;
    }
	
    ssize_t sendEOS() {
        const uint64_t szbe = htobe64((uint64_t)0);
        try {
            client->publish(out_topic, &szbe, HDR_SZ)->wait();
        } catch (...) {
            errno = ECOMM;
            return -1;
        }
        return (ssize_t)HDR_SZ;
    }

    ssize_t send(const void* buff, size_t size) {
        const uint64_t szbe = htobe64((uint64_t)size);
        std::string msg;
        msg.resize(HDR_SZ + size);
        memcpy(msg.data(), &szbe, HDR_SZ);
        if (size > 0) memcpy(msg.data() + HDR_SZ, buff, size);
        try {
            client->publish(out_topic, msg.data(), msg.size())->wait();
        } catch (...) {
            errno = ECOMM;
            return -1;
        }
        return (ssize_t)size;
	}

    ssize_t isend(const void* buff, size_t size, Request& r){
        const uint64_t szbe = htobe64((uint64_t)size);
        auto payload = std::make_shared<std::string>();
        payload->resize(HDR_SZ + size);
        memcpy(payload->data(), &szbe, HDR_SZ);
        if (size > 0) memcpy(payload->data() + HDR_SZ, buff, size);

        mqtt::delivery_token_ptr tok;
        try {
            tok = client->publish(out_topic, payload->data(), payload->size());
        } catch (...) {
            errno = ECOMM;
            return -1;
        }

        r.__setInternalR(new requestMQTTSend(std::move(tok), std::move(payload), (ssize_t)size));
        return (ssize_t)size;
    }

    ssize_t isend(const void* buff, size_t size, RequestPool& pool){
        const uint64_t szbe = htobe64((uint64_t)size);
        auto payload = std::make_shared<std::string>();
        payload->resize(HDR_SZ + size);
        memcpy(payload->data(), &szbe, HDR_SZ);
        if (size > 0) memcpy(payload->data() + HDR_SZ, buff, size);

        mqtt::delivery_token_ptr tok;
        try {
            tok = client->publish(out_topic, payload->data(), payload->size());
        } catch (...) {
            errno = ECOMM;
            return -1;
        }

        auto* slot = pool._getInternalVector<ConnRequestVectorMQTTSend>()->getNextSlot();
        slot->tok = std::move(tok);
        slot->payload = std::move(payload);
        return (ssize_t)size;
    }

    ssize_t receive(void* buff, size_t size){
		size_t probedSize;
		if (!probed.first){
			ssize_t r = probe(probedSize);
			if (r <= 0)	return r;
		} else {
			probedSize = probed.second;
		}
		
		if (probedSize == 0) {
			probed={false, 0};
			return 0;
		}
		if (probedSize > size){
			// Enforce "drain semantics": discard the whole message to allow subsequent receives.
            MTCL_MQTT_PRINT(100, "HandleMQTT::receive EMSGSIZE, buffer too small -> draining message\n");
            if (!drainCurrentMessage(probedSize)) {
                // errno set by drainCurrentMessage()
                probed = {false, 0};
                return -1;
            }
            probed = {false, 0};
            errno = EMSGSIZE;
            return -1;
		}

        while(true) {
            if(!messages.empty()) {
                auto msg = messages.front();
                messages.pop();

                // Ignore out-of-band connection ACKs that might arrive late.
                // ACK payloads are plain strings and are not framed (no HDR_SZ).
                if ((size_t)msg.second < HDR_SZ && msg.first == "ack") {
                    continue;
                }
				
				if (msg.second != (ssize_t)probedSize) {
					MTCL_MQTT_PRINT(100, "HandleMQTT::receive EPROTO, receiving less data or connection reset\n");
					errno=EPROTO;
					return -1;
				}				
				probed={false, 0};
				memcpy(buff, msg.first.data(), probedSize);
                return probedSize;
            }
			
            mqtt::const_message_ptr msg;
            bool res = client->try_consume_message(&msg);
            // A message has been received
            if(res) {
				if (!msg) {
					MTCL_MQTT_PRINT(100, "client reset connection\n");
					errno = ECONNRESET;
					return -1;
				}
				
                // This topic should have been subscribed upon creation of the
                // handle object
                if(msg->get_topic() == in_topic) {
					const auto& payload = msg->get_payload();

                    // Late ACKs are plain strings and must not be delivered to the user.
                    if (payload.size() < HDR_SZ && payload == "ack") {
                        continue;
                    }

                    if (payload.size() == probedSize) {
						probed={false, 0};
                        memcpy(buff, payload.data(), probedSize);
                        return (ssize_t)probedSize;
                    }
                    if (payload.size() < HDR_SZ) {
                        MTCL_MQTT_PRINT(100, "HandleMQTT::receive EPROTO, short packet\n");
                        errno = EPROTO;
                        return -1;
                    }
                    uint64_t szbe = 0;
                    memcpy(&szbe, payload.data(), HDR_SZ);
                    const size_t sz   = (size_t)be64toh(szbe);
					const size_t plsz = payload.size() - HDR_SZ;
                    if (sz != probedSize || plsz != probedSize) {
						MTCL_MQTT_PRINT(100, "HandleMQTT::receive EPROTO, size mismatch\n");
						errno = EPROTO;
						return -1;
                    }
					probed={false, 0};
                    memcpy(buff, payload.data() + HDR_SZ, probedSize);
                    return (ssize_t)probedSize;
				}
            }
			if constexpr (MQTT_POLL_TIMEOUT) 
				std::this_thread::sleep_for(std::chrono::milliseconds(MQTT_POLL_TIMEOUT));
        } 
        return 0;
    }

    ssize_t ireceive(void* buff, size_t size, RequestPool& pool){
        pool._getInternalVector<ConnRequestVectorMQTTRecv>()->getNextSlot(this, buff, size);
        return (ssize_t)size;
    }
	ssize_t ireceive(void* buff, size_t size, Request& r) {
        r.__setInternalR(new requestMQTTRecv(this, buff, size));
        return (ssize_t)size;
    }

    ssize_t probe(size_t& size, const bool blocking=true) {
		if (probed.first) {
			size = probed.second;
			return (size ? (ssize_t)HDR_SZ: 0);
		}
		while(!messages.empty()) {
			// It means the Manager put the message header in the queue for us
            auto msg = messages.front();
            messages.pop();

            // Ignore out-of-band connection ACKs that might arrive late
            if ((size_t)msg.second < HDR_SZ && msg.first == "ack") {
                continue;
            }

			if (msg.second < (ssize_t)HDR_SZ) {
				MTCL_MQTT_PRINT(100, "HandleMQTT::probe EPROTO, receiving less data or connection reset (messages)\n");

				probed = {true, 0};
				errno = EPROTO;
				return -1;
			}
            uint64_t szbe = 0;
            memcpy(&szbe, msg.first.data(), HDR_SZ);
            size = (size_t)be64toh(szbe);

            const size_t totsize = (size_t)msg.second;
            const size_t plsz    = totsize - HDR_SZ;
            if (plsz != size) {
                MTCL_MQTT_PRINT(100, "HandleMQTT::probe EPROTO, size mismatch\n");
                errno = EPROTO;
                return -1;
            }
            if (plsz > 0) {
                messages.push({msg.first.substr(HDR_SZ, plsz), (ssize_t)plsz});
            }
            probed = {true, size};
			return (size ? (ssize_t)HDR_SZ: 0);
		}

        mqtt::const_message_ptr msg;
		bool disconnected = false;
		auto try_get_header_msg = [&]() -> bool {
			// try to consume one message
			if (!client->try_consume_message(&msg)) {
				return false;
			}
			if (!msg) {
				disconnected = true;
				return false;
			}

			// Ignore out-of-band connection ACKs that might arrive late
			if (msg->get_topic() == in_topic) {
				const auto& payload = msg->get_payload();
				if (payload.size() < HDR_SZ && payload == "ack") {
					return false;
				}
			}
			// accept only the expected topic
			return (msg->get_topic() == in_topic);
		};

		if (!blocking) {
			if (!try_get_header_msg()) {
				if (disconnected || !client->is_connected()) {
					MTCL_MQTT_PRINT(100, "client reset connection\n");
					errno = ECONNRESET;
					return -1;
				}
				errno = EWOULDBLOCK;
				return -1;
			}
		} else {  // blocking
			while(!try_get_header_msg()) {
				if (disconnected || !client->is_connected()) {
					MTCL_MQTT_PRINT(100, "client reset connection\n");
					errno = ECONNRESET;
					return -1;
				}
				if constexpr (MQTT_POLL_TIMEOUT)
					std::this_thread::sleep_for(std::chrono::milliseconds(MQTT_POLL_TIMEOUT));
			}
		}
		const auto& payload = msg->get_payload();
		if (payload.size() < HDR_SZ) {
			MTCL_MQTT_PRINT(100, "HandleMQTT::probe EPROTO, receiving less data (%ld)\n", payload.size());
			errno = EPROTO;
			return -1;
		}
        uint64_t szbe = 0;
        memcpy(&szbe, payload.data(), HDR_SZ);
        size = (size_t)be64toh(szbe);

        const size_t plsz = payload.size() - HDR_SZ;
        if (plsz != size) {
            MTCL_MQTT_PRINT(100, "HandleMQTT::probe EPROTO, size mismatch\n");
            errno = EPROTO;
            return -1;
        }
        if (plsz > 0) {
            messages.push({payload.substr(HDR_SZ, plsz), (ssize_t)plsz});
        }
		probed = {true, size};
		return (size ? (ssize_t)HDR_SZ: 0);
    }

    bool peek() {
        size_t sz;
        ssize_t res = this->probe(sz, false);
        return res > 0;
    }

    ~HandleMQTT() {
        try {
            client->stop_consuming();
            if (client->is_connected()) client->disconnect()->wait();
        } catch (...) {}
        delete client;
    }

};



inline int requestMQTTRecv::test(int& result) {
    if (completed) { result = 1; return 0; }

    size_t sz = 0;
    const ssize_t pr = h->probe(sz, false);
    if (pr < 0) {
        if (errno == EWOULDBLOCK) { result = 0; return 0; }
        result = 0;
        return -1;
    }
    if (pr == 0) {
        // EOS
        completed = true;
		got = 0;
        h->clearProbed();
        result = 1;
        return 0;
    }

    if (sz > expected) {
		// Drain semantics: discard the message and return EMSGSIZE
		if (!h->drainCurrentMessage(sz)) {
            // errno set by drainCurrentMessage()
            h->clearProbed();
            completed = true;
            got = (ssize_t)sz;
            result = 1;
            return -1;
        }
        h->clearProbed();
        completed = true;
        got = (ssize_t)sz;   // record the real message size
        errno = EMSGSIZE;
        result = 1;          // completed (with error)
        return -1;
    }
    if (sz == 0) {
        completed = true;
		got = 0;
        h->clearProbed();
        result = 1;
        return 0;
    }

    // probe() already queued the payload in h->messages
    if (h->messages.empty()) { result = 0; return 0; }

    auto msg = h->messages.front();
    h->messages.pop();

    if ((size_t)msg.second != sz) {
        errno = EPROTO;
        result = 0;
        return -1;
    }

    memcpy(buff, msg.first.data(), sz);
    h->clearProbed();
    completed = true;
	got = (ssize_t)sz;
    result = 1;
    return 0;
}

inline int requestMQTTRecv::wait() {
	if (completed) return 0;

    // Use probe() in blocking mode to get the real size 
    size_t sz = 0;
	const ssize_t pr = h->probe(sz, true);
    if (pr < 0) { got = -1; return -1; }
    if (pr == 0) { // EOS
		h->clearProbed();
        completed = true;
        got = 0;
        return 0;
    }

    if (sz > expected) {
        if (!h->drainCurrentMessage(sz)) {
            h->clearProbed();
            completed = true;
            got = (ssize_t)sz;
            return -1; // errno already set (EPROTO)
        }
        h->clearProbed();
        completed = true;
        got = (ssize_t)sz;
        errno = EMSGSIZE;
        return -1;
    }

    // Now safe: receive() will consume the queued payload and clear probed
    const ssize_t rr = h->receive(buff, expected);
    if (rr < 0) { got = -1; return -1; }
    got = rr;
    completed = true;
    return 0;
}


class ConnMQTT : public ConnType {
private:
    std::string manager_name, new_connection_topic;
    std::string appName;
    size_t count = 0;

    bool createClient(mqtt::string topic, mqtt::async_client* aux_cli) {
        auto aux_connOpts = mqtt::connect_options_builder()
            .user_name(MQTT_USERNAME)
            .password(MQTT_PASSWORD)
            .keep_alive_interval(std::chrono::seconds(30))
            .automatic_reconnect(std::chrono::seconds(2), std::chrono::seconds(30))
            .clean_session(false)
            .finalize();

        try {
            aux_cli->connect(aux_connOpts)->wait();

            // listening on topic-in
            aux_cli->subscribe({topic}, {0})->wait();
            aux_cli->start_consuming();
        } catch (...) {
            MTCL_MQTT_PRINT(100, "ConnMQTT::createClient ERROR, cannot connect/subscribe");
            errno = ECOMM;
            return false;
        }
        return true;
    }

protected:
    std::string server_address{MQTT_SERVER_ADDRESS};
    mqtt::async_client*newConnClient;

    std::atomic<bool> finalized = false;
    bool listening              = false;
	bool newconn_consuming      = false;  // async_client consuming queue enabled
    
    std::map<HandleMQTT*, bool> connections;  // Active connections for this Connector
    REMOVE_CODE_IF(std::shared_mutex shm);

public:

   ConnMQTT(){};
   ~ConnMQTT(){};

    int init(std::string s) {
        appName = s + ":";
		
		char *addr;
		if ((addr=getenv("MQTT_SERVER_ADDRESS")) != NULL) {
			server_address = std::string(addr);
		}
        newConnClient = new mqtt::async_client(server_address, appName);
        auto connOpts = mqtt::connect_options_builder()
            .user_name(MQTT_USERNAME)
            .password(MQTT_PASSWORD)
            .keep_alive_interval(std::chrono::seconds(30))
            .automatic_reconnect(std::chrono::seconds(2), std::chrono::seconds(30))
            .clean_session(false)
            .finalize();

		try {
			newConnClient->connect(connOpts)->wait();
			// async_client needs start_consuming() before any (try_)consume.
            newConnClient->start_consuming();
            newconn_consuming = true;
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
		MTCL_MQTT_PRINT(100, "ConnMQTT::listen: connection topic %s\n", new_connection_topic.c_str());
		try {
			newConnClient->subscribe({new_connection_topic}, {0})->wait();
			if (!newconn_consuming) {
				newConnClient->start_consuming();
				newconn_consuming = true;
			}
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

        // Consume new-connection messages
        mqtt::const_message_ptr msg;
		if (listening && newconn_consuming) {
			newConnClient->try_consume_message(&msg);
		}
		
        if (msg) {
            // New message on new connection topic
            if(msg->get_topic() == new_connection_topic) {
				const std::string base = msg->to_string();
                const mqtt::string topic_in  = base + MQTT_IN_SUFFIX;
                const mqtt::string topic_out = base + MQTT_OUT_SUFFIX;
				// If the client retries with the same topic, avoid creating duplicates.
                bool already = false;
                REMOVE_CODE_IF(ulock.lock());
                for (auto &kv : connections) {
                    auto* h = kv.first;
                    if (h && h->in_topic == topic_in && h->out_topic == topic_out) {
                        already = true;
                        break;
                    }
                }
                REMOVE_CODE_IF(ulock.unlock());

                if (already) {
                    try {
                        auto ack = mqtt::make_message(topic_out, "ack");
                        ack->set_qos(1);
                        newConnClient->publish(ack)->wait();
                    } catch (...) {}
                } else {
                    mqtt::async_client* aux_cli = new mqtt::async_client(server_address, topic_in);
                    if (!createClient(topic_in, aux_cli)) {
                        delete aux_cli;
                        return;
                    }

                    // Must send the ACK to the remote end otherwise we can miss
                    // messages published immediately after the connection request.
                    auto ack = mqtt::make_message(topic_out, "ack");
                    ack->set_qos(1);
                    aux_cli->publish(ack)->wait();

                    HandleMQTT* handle = new HandleMQTT(this, aux_cli, topic_out, topic_in);

                    REMOVE_CODE_IF(ulock.lock());
                    connections.insert({handle, false});
                    REMOVE_CODE_IF(ulock.unlock());
					// Never call Manager::addinQ() while holding the protocol lock (shm).
                    addinQ(true, handle);
                }
            }
        }
        else if (listening && newconn_consuming) {
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
		
		std::vector<HandleMQTT*> ready;
		REMOVE_CODE_IF(ulock.lock());
        for (auto &[handle, to_manage] : connections) {
            if(to_manage) {
                mqtt::const_message_ptr msg;
                const bool res = handle->client->try_consume_message(&msg);
				if (!res) continue;
				if (!msg) { 
					handle->messages.push({std::string(), 0});
				} else {
					handle->messages.push({msg->get_payload(), (ssize_t)msg->get_payload().length()});
				}
				to_manage = false;
				ready.push_back(handle);
            }
        }
		REMOVE_CODE_IF(ulock.unlock());
		for(auto* h: ready) addinQ(false, h);
    }


    // String for connection composed of manager_id:topic
    Handle* connect(const std::string& address, int retry, unsigned timeout_ms) {
        std::string manager_id = address.substr(0, address.find(":"));

        if (manager_id.empty()) {
            MTCL_MQTT_PRINT(100, "ConnMQTT::connect: server connect string must be defined\n");
            errno = EINVAL;
            return nullptr;
        }

		// Defaults if caller does not specify them
        const int ntries = (retry < 0) ? CCONNECTION_RETRY : (retry == 0 ? 1 : retry);
        const unsigned step_ms = (timeout_ms == 0) ? CCONNECTION_TIMEOUT : timeout_ms;

        // Keep the same topic/client-id across retries.
        const std::string topic = appName + std::to_string(count++);
        const mqtt::string topic_out = topic + MQTT_OUT_SUFFIX;
        const mqtt::string topic_in  = topic + MQTT_IN_SUFFIX;

        mqtt::connect_options connOpts;
		connOpts.set_keep_alive_interval(20);
		connOpts.set_clean_session(true);
        connOpts.set_user_name(MQTT_USERNAME);
        connOpts.set_password(MQTT_PASSWORD);

		mqtt::async_client* client = new mqtt::async_client(server_address, topic);
		try {
            client->connect(connOpts)->wait();
            client->subscribe({topic_out}, {0})->wait();
            client->start_consuming();
        } catch (...) {
            errno = ECOMM;
            try {
                client->stop_consuming();
                if (client->is_connected()) client->disconnect()->wait();
            } catch (...) {}
            delete client;
            return nullptr;
        }
		auto pubmsg = mqtt::make_message(manager_id + MQTT_CONNECTION_TOPIC, topic);
        pubmsg->set_qos(1);

		// During connection establishment we must consume only the control ACK.
		// Any other message arriving on the same subscription (for example due to
		// QoS mixing or broker reordering) must be preserved and delivered later
		// through the HandleMQTT message queue, otherwise the stream gets desynchronized.
		std::vector<std::pair<std::string, ssize_t>> early_msgs;
		mqtt::const_message_ptr ack;
        bool got_ack = false;
        for (int attempt = 0; attempt < ntries && !got_ack; ++attempt) {
            try {
                client->publish(pubmsg)->wait();
            } catch (...) {
                errno = ECOMM;
                break;
            }
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(step_ms);
            const auto spin_until = std::chrono::steady_clock::now() + std::chrono::microseconds(200);
            auto sleep = std::chrono::microseconds(1);
            const auto max_sleep = std::chrono::microseconds(200);

            while (std::chrono::steady_clock::now() < deadline) {
                mqtt::const_message_ptr m;
                const bool res = client->try_consume_message(&m);
                if (res && m) {
					const auto& payload = m->get_payload();
					if (payload.size() < HandleMQTT::HDR_SZ && payload == "ack") {
						ack = m;
						got_ack = true;
						break;
					}
					// Not an ACK.... 
					early_msgs.emplace_back(payload, (ssize_t)payload.size());
                }

                if (std::chrono::steady_clock::now() < spin_until) {
                    mtcl_cpu_relax();
                } else {
                    std::this_thread::sleep_for(sleep);
                    sleep = sleep + sleep;
                    if (sleep > max_sleep) sleep = max_sleep;
                }
            }

            if (!got_ack && attempt + 1 < ntries) {
                std::this_thread::sleep_for(std::chrono::milliseconds(step_ms));
            }
        }

        if (!got_ack) {
            if (errno == 0) errno = ETIMEDOUT;
            try {
                client->stop_consuming();
                if (client->is_connected()) client->disconnect()->wait();
            } catch (...) {}
            delete client;
            return nullptr;
        }

        // We write on topic + MQTT_IN_SUFFIX and we read from topic + MQTT_OUT_SUFFIX
        HandleMQTT* handle = new HandleMQTT(this, client, topic_in, topic_out, true);
		// Preserve any early data/control messages observed during connect.
		// They will be parsed by probe()/receive() as usual.
		for (auto& p : early_msgs) {
			handle->messages.push(std::move(p));
		}
        {
            REMOVE_CODE_IF(std::unique_lock lock(shm));
            connections[handle] = false;
        }
		
        MTCL_MQTT_PRINT(100, "connected to: %s\n", (manager_id + MQTT_CONNECTION_TOPIC).c_str());
        return handle;
    }

    void notify_close(Handle* h, bool close_wr=true, bool close_rd=true) {
        REMOVE_CODE_IF(std::unique_lock l(shm));		
        HandleMQTT* handle = reinterpret_cast<HandleMQTT*>(h);

        if(close_rd)
            connections.erase(handle);
    }


    void notify_yield(Handle* h) override {
        HandleMQTT* handle = reinterpret_cast<HandleMQTT*>(h);
		bool enqueue_now = false;
		{
			REMOVE_CODE_IF(std::unique_lock l(shm));
			auto it = connections.find(handle);
			if(it == connections.end()) {
				MTCL_MQTT_ERROR("Couldn't yield handle\n");
				return;
			}
			// the handle is already IO-managed, skip
			if (it->second) return;

			// already something in the message buffer?
			if (!handle->messages.empty()) {
				enqueue_now = true;
			} else {
				// Check if handle still has some data to receive.
				size_t size;
				errno = 0;
				const ssize_t r = handle->probe(size, false);
				if (r >= 0) {
					enqueue_now = true;
				} else if (errno != EWOULDBLOCK && errno != EAGAIN) {
					// propagate the error to the user level by re-queuing the handle
					enqueue_now = true;
				}
			}
			// If nothing is ready now, delegate polling to the IO thread.
			if (!enqueue_now) it->second = true;
 		}
		if (enqueue_now) addinQ(false, handle);
    }

    void end(bool blockflag=false) {
        auto modified_connections = connections;
        for(auto& [handle, to_manage] : modified_connections)
            if(to_manage)
                setAsClosed(handle, blockflag);

        delete newConnClient;
    }

};

} // namespace
