#include <iostream>
#include <fstream>
#include <string>
#include <optional>
#include <thread>

#include "rapidjson/rapidjson.h"
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/document.h>

#include "mtcl.hpp"

//#define PROXYPROXYMQTT

#ifdef PROXYPROXYMQTT
#include "protocols/mqtt.hpp"
#endif

#define PROXY_CLIENT_PORT 13000
#define PROXY_CLIENT_PORT_UCX 13001
#define PROXY_PORT 8002 // solo tra proxy
#define MAX_DEST_STRING 60
#define CHUNK_SIZE 1200

enum cmd_t : char {FWD = 0, CONN = 1, PRX = 2, ERR_CONN = 3, EOS = 4, CONN_COLL = 2};

/**
 * PROXY <--> PROXY PROTOCOL
 *  | char CMD |  size_t IDENTIFIER |  ....... PAYLOAD ........ |
*/
char headerBuffer[sizeof(char) + sizeof(size_t)];
char chunkBuffer[CHUNK_SIZE];

using connID_t = size_t;
using handleID_t = size_t;

// config file
// pools: Name => [List(proxyIP), List(nodes)]
std::map<std::string, std::pair<std::vector<std::string>, std::vector<std::string>>> pools;
// components: Name => [hostname, List(protocols), List(listen_endpoints)]
std::map<std::string, std::tuple<std::string, std::vector<std::string>, std::vector<std::string>>> components;


std::map<handleID_t, HandleUser> id2handle; // dato un handleID ritorna l'handle

std::map<connID_t, HandleUser*> connid2proxy; // dato una connID (meaningful tra proxy) ritorna l'handle del proxy su cui scrivere
std::map<handleID_t, connID_t> loc2connID;  // associazione bidirezionale handleID <-> conneID
std::map<connID_t, handleID_t> connID2loc;
// singolo hop PROC <--> Proxy <--> Proc
std::map<handleID_t, handleID_t> proc2proc; // associazioni handleID <-> handleID per connessioni tra processi mediante singolo proxy


// invia un messaggio ad un proxy formattato secondo il protocollo definito
/*void sendHeaderProxy(HandleUser& h, cmd_t cmd, size_t identifier){
    headerBuffer[0] = cmd;
    memcpy(headerBuffer+sizeof(char), &identifier, sizeof(size_t)); 
    h.send(headerBuffer, sizeof(headerBuffer));
}*/


HandleUser* toHeap(HandleUser h){
    return new HandleUser(std::move(h));
}

 template <bool B, typename T>   
std::vector<std::string> JSONArray2VectorString(const rapidjson::GenericArray<B,T>& arr){
    std::vector<std::string> output;
    for(auto& e : arr)  
        output.push_back(e.GetString());
    
    return output;
}

int parseConfig(const std::string& f){
    std::ifstream ifs(f);
    if ( !ifs.is_open() ) {
        MTCL_PRINT(0, "[Manager]:\t", "parseConfig: cannot open file %s for reading, skip it\n",
                    f.c_str());
        return -1;
    }
    rapidjson::IStreamWrapper isw { ifs };
    rapidjson::Document doc;
    doc.ParseStream( isw );

    assert(doc.IsObject());

    if (doc.HasMember("pools") && doc["pools"].IsArray()){
        // architecture 
        for (auto& c : doc["pools"].GetArray())
            if (c.IsObject() && c.HasMember("name") && c["name"].IsString() && c.HasMember("proxyIp") && c["proxyIp"].IsArray() && c.HasMember("nodes") && c["nodes"].IsArray()){
                auto name = c["name"].GetString();
                if (pools.count(name))
                    MTCL_ERROR("[Manager]:\t", "parseConfig: one pool element is duplicate on configuration file. I'm overwriting it.\n");
                
                pools[name] = std::make_pair(JSONArray2VectorString(c["proxyIp"].GetArray()), JSONArray2VectorString(c["nodes"].GetArray()));
            } else
                MTCL_ERROR("[Manager]:\t", "parseConfig: an object in pool is not well defined. Skipping it.\n");
    }

    if (doc.HasMember("components") && doc["components"].IsArray()){
        // components
        for(auto& c : doc["components"].GetArray())
            if (c.IsObject() && c.HasMember("name") && c["name"].IsString() && c.HasMember("host") && c["host"].IsString() && c.HasMember("protocols") && c["protocols"].IsArray()){
                auto name = c["name"].GetString();
                if (components.count(name))
                    MTCL_ERROR("[Manager]:\t", "parseConfig: one component element is duplicate on configuration file. I'm overwriting it.\n");
                
                auto listen_strs = (c.HasMember("listen-endpoints") && c["listen-endpoints"].IsArray()) ? JSONArray2VectorString(c["listen-endpoints"].GetArray()) : std::vector<std::string>();
                components[name] = std::make_tuple(c["host"].GetString(), JSONArray2VectorString(c["protocols"].GetArray()), listen_strs);
            } else
                    MTCL_ERROR("[Manager]:\t", "parseConfig: an object in components is not well defined. Skipping it.\n");
    }
    return 0;
}

int main(int argc, char** argv){
    if (argc < 3){
        std::cerr << "Usage: " << argv[0] << " poolName  configFile" << std::endl;
        return -1;
    }

    std::string pool(argv[1]);

#ifdef PROXYPROXYMQTT
    Manager::registerType<ConnMQTT>("P");
#else
    Manager::registerType<ConnTcp>("P");
#endif

    Manager::init("PROXY-"+pool);

    // parse file config
    if (parseConfig(std::string(argv[2])) < 0)
        return -1;

    Manager::listen("TCP:0.0.0.0:" + std::to_string(PROXY_CLIENT_PORT));
    Manager::listen("MQTT:PROXY-" + pool);
    Manager::listen("MPIP2P:PROXY-" + pool);
    Manager::listen("UCX:0.0.0.0:" + std::to_string(PROXY_CLIENT_PORT_UCX));

#ifdef PROXYPROXYMQTT
    Manager::listen("P:PROXYPROXY-" + pool);
#else
    Manager::listen("P:0.0.0.0:" + std::to_string(PROXY_PORT));
#endif
    // check if the passed pool as argument actually exists in the configuration file
    if (!pools.count(pool)){
        std::cerr << "Pool not found in configuration File!\n";
        return -1;
    }

    // connect to other proxies
    std::map<std::string, HandleUser*> proxies;
    for(auto& [name, val] : pools)
        if (name > pool) {
            for(auto& addr : val.first){
                ///if (add == mioaddr) continue; ## TODO!!
                
#ifdef PROXYPROXYMQTT
                auto h = Manager::connect("P:PROXYPROXY-"+name);
#else
                // check if there is a ":", it means there is a port_; in this case do not add the default PROXY_PORT
                auto h = Manager::connect("P:" + addr + (addr.find(":") == std::string::npos ? ":" + std::to_string(PROXY_PORT) : ""));
#endif
                if (h.isValid()) {
                    MTCL_PRINT(0, "[PROXY]"," Connected to PROXY of %s (connection string: %s)\n", name.c_str(), addr.c_str());
                    
                    // send cmd: PRX - ID: 0 - Payload: {pool name} to the just connected proxy
                    char* buff = new char[sizeof(cmd_t) + sizeof(size_t) + pool.length()];
                    buff[0] = cmd_t::PRX;
                    memset(buff+1, 0, sizeof(size_t));
                    memcpy(buff+sizeof(char)+sizeof(size_t), pool.c_str(), pool.length());
                    h.send(buff, sizeof(cmd_t) + sizeof(size_t) + pool.length());

                    h.yield();
                    // save the proxy handle to perform future writes
                    proxies[name] = toHeap(std::move(h));
                    break;
                } else {
                    MTCL_PRINT(0, "[PROXY]","[ERROR] Cannot connect to PROXY of %s (connection string: %s)\n", name.c_str(), addr.c_str());
                }
            }
        }

    // this is kind of an event loop
    while(true){
        auto h = Manager::getNext();
        
        // the handle represent a PROXY-2-PROXY connection
        if (Manager::getTypeOfHandle(h) == "P"){
            if (h.isNewConnection()){ // new incoming PROXY connection
                MTCL_PRINT(0, "[PROXY]", "Received a new connection from proxy (before reading)\n");

                size_t sz;
                if (h.probe(sz, true) <= 0){
                    MTCL_PRINT(0, "[PROXY][ERROR]", "Probe error on new connection from proxy\n");
                }
                char* buff = new char[sz+1];
                h.receive(buff, sz);
                buff[sz] = '\0';
                std::string poolName;
                if (buff[0] == cmd_t::PRX) //if cmd is PRX, read the poolName from the payload
                    poolName = std::string(buff+sizeof(size_t)+sizeof(char));
                
                // yield the connection and save the handle to perform future writes
                h.yield();
                proxies[poolName] = toHeap(std::move(h));
                delete [] buff;
                MTCL_PRINT(0, "[PROXY]", "Received a new connection from proxy of pool: %s\n", poolName.c_str());
                continue;
            }

            size_t sz;
            if (h.probe(sz) < 0){
                MTCL_PRINT(0, "[PROXY][ERROR]", "Probe error on receive form proxy\n");
            };
            char* buff = new char[sz];
            h.receive(buff, sz);

            // parse the PROXY-2-PROXY header fields
            cmd_t cmd = (cmd_t)buff[0];
            connID_t identifier = *reinterpret_cast<connID_t*>(buff+sizeof(char));
            char* payload = buff + sizeof(char) + sizeof(size_t);
            size_t size = sz - sizeof(char) - sizeof(size_t); // actual payload size

            if (cmd == cmd_t::EOS){
                MTCL_PRINT(0, "[PROXY]", "Received a EOS from a remote peer\n");

                if (connID2loc.count(identifier)){
                    handleID_t hID_ = connID2loc.at(identifier);
                    auto& h_ = id2handle.at(hID_);
                    h_.close();
                    if (h_.isClosed() == std::make_pair<bool, bool>(true, true)){
                        id2handle.erase(hID_);

                        connID2loc.erase(loc2connID[hID_]);
                        loc2connID.erase(hID_);
                    }
                }
                else
                    std::cerr << "Received a EOS message from a proxy but the identifier is unknown!\n";
            }
           
           if (cmd == cmd_t::FWD){
                if (connID2loc.count(identifier) && id2handle.count(connID2loc.at(identifier)))
                    id2handle[connID2loc.at(identifier)].send(payload, size);
                else
                    std::cerr << "Received a forward message from a proxy but the identifier is unknown! Identifier: " << identifier << " - Payload: " << payload << std::endl;
                std::cerr << "FWD performed!\n";
           }

           if (cmd == cmd_t::CONN || cmd == cmd_t::CONN_COLL){
                std::string connectionString(payload, size); // something like: TCP:Appname, UCX:AppName
                std::string protocol;
                std::string componentName;
                if (connectionString.find(":") != string::npos){
                    // there is no protocol
                     protocol = connectionString.substr(0, connectionString.find(':')); // just protocol without component name
                     componentName = connectionString.substr(connectionString.find(':')+1); // just component name without protocol
                } else {
                    componentName = connectionString;
                }

                MTCL_PRINT(0, "[PROXY]", "Received a %s connection directed to %s with protocol %s\n", (cmd == cmd_t::CONN_COLL ? "collective" : "p2p"), componentName.c_str(), protocol.c_str());
                // check that the component name actually exists in the configuration file
                if (!components.count(componentName)){
                    std::cerr << "Component name ["<< componentName << "] not found in configuration file\n";
                    continue;
                }

                // retrieve the list of endpoints in which the destination is listening on
                auto& componentInfo = components[componentName];
                std::vector<std::string>& listen_endpoints = std::get<2>(componentInfo);

                bool found = false;
                for (auto& le : listen_endpoints)
                    if (protocol.empty() || le.find(protocol) != std::string::npos){
                        auto newHandle = Manager::connect(le); // connect to the final destination directly following the protocol specified
                        if (newHandle.isValid()){
                            loc2connID.insert(std::make_pair(newHandle.getID(), identifier));
                            connID2loc.insert(std::make_pair(identifier, newHandle.getID()));
                            /// ########
                            int collective = cmd == cmd_t::CONN_COLL;
                            newHandle.send(&collective, sizeof(int)); // <== send the int for a collective
                            /// ########
                            newHandle.yield();
                            id2handle.emplace(newHandle.getID(), std::move(newHandle));
                            h.yield();
                            connid2proxy.emplace(identifier, toHeap(std::move(h)));
                            found = true;
                            break;
                        }
                    }
                
                if (!found){
                    std::cerr << "Protocol specified ["<<protocol<<"] not supported by the remote peer ["<< componentName <<"]\n";
                    // TODO: manda indietro errore al proxy di orgine...
                }
                std::cout << "[PROXY] connection forwarded to the process!\n";
           }
            
            delete [] buff;
        
            continue;
        } else { // receive something from a component (NOT A PROXY!)
            if (h.isNewConnection()){
                // read destination PORTOCOL:ComponentName

                size_t sz;
                if (h.probe(sz) <= 0){
                    MTCL_PRINT(0, "[PROXY][ERROR]", "Probe return 0 or -1 for a new connection not from a proxy\n");
                    continue;
                }

                char* destComponentName = new char[sz];
                h.receive(destComponentName, sz);

                int collective = 0;		
                if (h.receive(&collective, sizeof(int)) <= 0){
                    MTCL_PRINT(0, "[PROXY][ERROR]", "Probe return 0 or -1 for a new connection not from a proxy\n");
                    continue;
                }
                char* teamID = nullptr;
                size_t teamIDSize = 0;
                if(collective) {
                
                    if (h.probe(teamIDSize, true) <= 0) {
                        MTCL_PRINT(0, "[Manager]:\t", "addinQ handshProtocol specifiedake error in probe, teamID size, errno=%d\n", errno);
                        teamID=nullptr;
                        return -1;
                    }
                    // sanity check
                    if (teamIDSize>1048576) {
                        MTCL_PRINT(0, "[Manager]:\t", "addinQ handshake error in probe, teamID size TOO LARGE (size=%ld)\n", teamIDSize);
                        teamID=nullptr;
                        return -1;
                    }
                    
                    teamID = new char[teamIDSize+1];
                    assert(teamID);
                    if (h.receive(teamID, teamIDSize) <=0) {
                        MTCL_PRINT(0, "[Manager]:\t", "addinQ handshake error in probe, receiving teamID, errno=%d\n", errno);
                        delete [] teamID;
                        teamID=nullptr;
                        return -1;
                    }
                    teamID[teamIDSize] = '\0';			
                    MTCL_PRINT(100, "[PROXY]: \t", "received connection for team: %s\n", teamID);
                }	

                std::string connectString(destComponentName, sz);
                std::string componentName = connectString.substr(connectString.find(':')+1);

                MTCL_PRINT(0, "[PROXY]", "Recieved a connection directed to %s\n", connectString.c_str());

                if (!components.count(componentName)){
                    std::cerr << "Component name ["<< componentName << "] not found in configuration file\n";
                    continue;
                }

                auto& componentInfo = components[componentName];
                std::string& hostname = std::get<0>(componentInfo);
                std::string poolOfDestination = hostname.substr(0, hostname.find(':'));
                
                if (poolOfDestination.empty() || poolOfDestination == pool){ 
                    // desrtinazione visibile direttamente dal proxy JUST ONE HOP!!!
                    std::string protocol = connectString.substr(0, connectString.find(':'));
                    std::vector<std::string>& listen_endpoints = std::get<2>(componentInfo);
                    if (protocol.empty()){
                        // TODO: pigliane uno a caso che supporto anche io
                        // for for (auto& le : listen_endpoints) connect se ok bene!

                        // per ora ce sempre
                    } else {
                        bool found = false;
                        for (auto& le : listen_endpoints)
                            if (le.find(protocol) != std::string::npos){
                                auto newHandle = Manager::connect(le);
                                if (newHandle.isValid()){
                                    // ###########
                                    newHandle.send(&collective, sizeof(int));
                                    if (collective) newHandle.send(teamID, teamIDSize); // send teamID if it is a collective
                                    // ############
                                    proc2proc.emplace(h.getID(), newHandle.getID());
                                    proc2proc.emplace(newHandle.getID(), h.getID());
                                    newHandle.yield();
                                    id2handle.emplace(newHandle.getID(), std::move(newHandle));
                                    found = true;
                                    break;
                                }
                            }
                        
                        if (!found){
                           std::cerr << "Protocol specified ["<<protocol<<"] not supported by the remote peer ["<< componentName <<"]\n";
                           h.close();
                           continue; 
                        }
                    }
                } else { // pool of destination non-empty
                    std::cout << "The connection is actually a multi-hop proxy communication\n";
                    char* buff = new char[sizeof(cmd_t)+sizeof(handleID_t)+connectString.length()];
                    buff[0] = collective ? cmd_t::CONN_COLL : cmd_t::CONN;
                    connID_t identifier = std::hash<std::string>{}(connectString + pool + std::to_string(h.getID()));
                    memcpy(buff+sizeof(cmd_t), &identifier, sizeof(connID_t));
                    memcpy(buff+sizeof(cmd_t)+sizeof(connID_t), connectString.c_str(), connectString.length());
                    if (!proxies.count(poolOfDestination)){
                        MTCL_PRINT(0, "[PROXY]", "Pool of destination [%s] not found in the list of available pools\n", poolOfDestination.c_str());
                        continue; // check if its enough to continue
                    }
                    proxies[poolOfDestination]->send(buff, sizeof(cmd_t)+sizeof(handleID_t)+connectString.length());
                    delete [] buff;
                    loc2connID.insert(std::make_pair(h.getID(), identifier));
                    connID2loc.insert(std::make_pair(identifier, h.getID()));
                    connid2proxy.emplace(identifier, proxies[poolOfDestination]);
                    sleep(1);
                    // send the teamID if it is a collective
                    if (collective){
                        char* buff_ = new char[sizeof(cmd_t)+sizeof(handleID_t)+teamIDSize];
                        buff_[0] = cmd_t::FWD;
                        memcpy(buff_+sizeof(cmd_t), &identifier, sizeof(connID_t));
                        memcpy(buff_+sizeof(cmd_t)+sizeof(connID_t), teamID, teamIDSize);
                        proxies[poolOfDestination]->send(buff_, sizeof(cmd_t)+sizeof(handleID_t)+teamIDSize);
                        delete [] buff_;
                    }
                }

                h.yield();
                id2handle.emplace(h.getID(), std::move(h));
		        continue;
            }

            // receive something from a component but its not a new connection

            handleID_t connId = h.getID();
            size_t sz;
            if (h.probe(sz) < 0){
                MTCL_PRINT(0, "[PROXY][ERROR]", "Probe error on receive form direct client\n");
            };
            if (sz == 0){
                std::cout << "Received EOS from a direct client\n";
                if (loc2connID.count(connId)){ // if the connection is a multi hop send EOS cmd to the next proxy and cleanup 
                    char buffer[sizeof(cmd_t)+sizeof(connID_t)];
                    buffer[0] = cmd_t::EOS;
                    connID_t connectionID = loc2connID.at(connId);
                    memcpy(buffer+sizeof(cmd_t), &connectionID, sizeof(connID_t));
                    connid2proxy[connectionID]->send(buffer, sizeof(buffer));

                    // if the connection is closed both side we can cleanup everything related to the connection
                    if (h.isClosed() == std::make_pair(true, true)){
                        connID2loc.erase(loc2connID[connId]);
                        loc2connID.erase(connId);
                        connid2proxy.erase(connectionID);
                        id2handle.erase(connId);
                    }
                } else { // the connection is a single hop 
                    const auto& dest = proc2proc.find(connId);
                    if (dest != proc2proc.end()){
                        id2handle[dest->second].close();

                        if (id2handle[dest->second].isClosed() == std::make_pair(true, true)){
                            proc2proc.erase(dest->second);
                            id2handle.erase(dest->second);
                        }
                    }
                }

                continue;
            }

            char* buffer = new char[sizeof(cmd_t)+sizeof(connID_t)+sz];
            h.receive(buffer+sizeof(cmd_t)+sizeof(connID_t), sz); // write on the right side of the buffer

            if (loc2connID.count(connId)){
                buffer[0] = cmd_t::FWD;
                connID_t connectionID = loc2connID.at(connId);
                memcpy(buffer+sizeof(cmd_t), &connectionID, sizeof(connID_t));
                connid2proxy[connectionID]->send(buffer, sizeof(cmd_t)+sizeof(connID_t)+sz);
                delete [] buffer;
                continue;
            }
            const auto& dest = proc2proc.find(connId);
            if (dest != proc2proc.end()){
                id2handle[dest->second].send(buffer+sizeof(cmd_t)+sizeof(connID_t), sz);
                delete [] buffer;
                continue;
            }

            std::cerr << "Received something from a old connection that i cannot handle! :(\n";
            delete [] buffer;
        }
    }

    Manager::finalize(true);
    return 0;
}
