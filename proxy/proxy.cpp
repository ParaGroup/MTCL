#include <iostream>
#include <fstream>
#include <string>
#include <optional>
#include <thread>

#include "rapidjson/rapidjson.h"
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/document.h>
#include "bimap.hpp"

#include "../mtcl.hpp"
#define PROXY_CLIENT_PORT 13000
#define PROXY_CLIENT_PORT_UCX 13001
#define PROXY_PORT 8002 // solo tra proxy
#define MAX_DEST_STRING 60
#define CHUNK_SIZE 1200

enum cmd_t : char {FWD = 0, CONN = 1, PRX = 2, ERR_CONN = 3, EOS = 4};

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
bimap<handleID_t, connID_t> loc2connID;  // associazione bidirezionale handleID <-> conneID

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

void parseConfig(const std::string& f){
    std::ifstream ifs(f);
    if ( !ifs.is_open() ) {
        MTCL_ERROR("[Manager]:\t", "parseConfig: cannot open file %s for reading, skip it\n",
                    f.c_str());
        return;
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
}
/*
    Connect verso proxy ---> pool e ip
    Accept da proxy ---> ricevo pool -> 

*/

int main(int argc, char** argv){
    std::string pool(argv[1]);
    Manager::registerType<ConnTcp>("P");
    Manager::init("PROXY");

    // parse file config che prendo da argv[2]
    parseConfig(std::string(argv[2]));
    Manager::listen("TCP:0.0.0.0:" + std::to_string(PROXY_CLIENT_PORT));
    Manager::listen("MQTT:PROXY-" + pool);
    Manager::listen("MPIP2P:PROXY-" + pool);
    Manager::listen("UCX:0.0.0.0:" + std::to_string(PROXY_CLIENT_PORT_UCX));
    Manager::listen("P:0.0.0.0:" + std::to_string(PROXY_PORT));

    

    // check esistenza pool
    if (!pools.count(pool)){
        std::cerr << "Errore, pool non trovato!\n";
        abort();
    }

    // connect to other proxies
    std::map<std::string, HandleUser*> proxies;
    for(auto& [name, val] : pools)
        if (name > pool) {
            for(auto& addr : val.first){
                ///if (add == mioaddr) continue; ## TODO!!
                
                // check if there is a ":", it means there is a port_; in this case do not add the default PROXY_PORT
                auto h = Manager::connect("P:" + addr + (addr.find(":") == std::string::npos ? ":" + std::to_string(PROXY_PORT) : ""));
                if (h.isValid()) {

                    // send cmd: PRX - ID: 0 - Payload: {pool name}
                    char* buff = new char[sizeof(cmd_t) + sizeof(size_t) + pool.length()];
                    buff[0] = cmd_t::PRX;
                    memset(buff+1, 0, sizeof(size_t));
                    memcpy(buff+sizeof(char)+sizeof(size_t), pool.c_str(), pool.length());
                    h.send(buff, sizeof(cmd_t) + sizeof(size_t) + pool.length());

                    h.yield();
                    proxies[name] = toHeap(std::move(h));
                }
            }
        }

    while(true){
        auto h = Manager::getNext();
        
        if (Manager::getTypeOfHandle(h) == "P"){
            if (h.isNewConnection()){
                // nuovo proxy
                size_t sz;
                h.probe(sz, true);
                char* buff = new char[sz+1];
                h.receive(buff, sz);
                buff[sz] = '\0';
                std::string poolName;
                if (buff[0] == cmd_t::PRX)
                    poolName = std::string(buff+sizeof(size_t)+sizeof(char));
                
                proxies[poolName] = toHeap(std::move(h));
                delete [] buff;
                continue;
            }
            size_t sz;
            h.probe(sz);
            char* buff = new char[sz];
            h.receive(buff, sz);


            cmd_t cmd = (cmd_t)buff[0];
            connID_t identifier = *reinterpret_cast<connID_t*>(buff+sizeof(char));

            
            char* payload = buff + sizeof(char) + sizeof(size_t);
            size_t size = sz - sizeof(char) - sizeof(size_t);

            if (cmd == cmd_t::EOS){
                if (loc2connID.has_value(identifier)){
                    handleID_t hID_ = loc2connID.get_key(identifier);
                    auto& h_ = id2handle.at(hID_);
                    h_.close();
                    if (h_.isClosed() == std::make_pair<bool, bool>(true, true)){
                        id2handle.erase(hID_);
                        loc2connID.erase_key(hID_);
                    }
                }
                else
                    std::cerr << "Received a EOS message from a proxy but the identifier is unknown!\n";
            }
           
           if (cmd == cmd_t::FWD){
                if (loc2connID.has_value(identifier))
                    id2handle[loc2connID.get_key(identifier)].send(payload, size);
                else
                    std::cerr << "Received a forward message from a proxy but the identifier is unknown!\n";
           }

           if (cmd == cmd_t::CONN){
                std::string connectionString(payload, size); // TCP:Appname
                std::string componentName = connectionString.substr(connectionString.find(':')+1);

                if (!components.count(componentName)){
                    std::cerr << "Component name ["<< componentName << "] not found in configuration file\n";
                    continue;
                }

                auto& componentInfo = components[componentName];
                std::string protocol = connectionString.substr(0, connectionString.find(':'));
                std::vector<std::string>& listen_endpoints = std::get<2>(componentInfo);

                bool found = false;
                for (auto& le : listen_endpoints)
                    if (le.find(protocol) != std::string::npos){
                        auto newHandle = Manager::connect(le);
                        if (newHandle.isValid()){
                            loc2connID.insert(newHandle.getID(), identifier);
                            newHandle.yield();
                            id2handle.emplace(newHandle.getID(), std::move(newHandle));
                            found = true;
                            break;
                        }
                    }
                
                if (!found){
                    std::cerr << "Protocol specified ["<<protocol<<"] not supported by the remote peer ["<< componentName <<"]\n";
                    // TODO: manda indietro errore al proxy di orgine 
                }
           }
            
            delete [] buff;
        
            continue;
        } else {
            if (h.isNewConnection()){
                // read destination PORTOCOL:ComponentName

                size_t sz;
                h.probe(sz);
                char* destComponentName = new char[sz];
                h.receive(destComponentName, sz);

                std::string connectString(destComponentName, sz);
                std::string componentName = connectString.substr(connectString.find(':')+1);

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
                    char* buff = new char[sizeof(cmd_t)+sizeof(handleID_t)+connectString.length()];
                    buff[0] = cmd_t::CONN;
                    connID_t identifier = std::hash<std::string>{}(connectString + pool + std::to_string(h.getID()));
                    memcpy(buff+sizeof(cmd_t), &identifier, sizeof(connID_t));
                    memcpy(buff+sizeof(cmd_t)+sizeof(connID_t), connectString.c_str(), connectString.length());
                    proxies[poolOfDestination]->send(buff, sizeof(cmd_t)+sizeof(handleID_t)+connectString.length());
                    loc2connID.insert(h.getID(), identifier);
                    connid2proxy.emplace(identifier, proxies[poolOfDestination]);
                }

                h.yield();
                id2handle.emplace(h.getID(), std::move(h));
		        continue;
            }

            handleID_t connId = h.getID();
            size_t sz;
            h.probe(sz);
            if (sz == 0){

                if (loc2connID.has_key(connId)){
                    char buffer[sizeof(cmd_t)+sizeof(connID_t)];
                    buffer[0] = cmd_t::EOS;
                    connID_t connectionID = loc2connID.get_value(connId);
                    memcpy(buffer+sizeof(cmd_t), &connectionID, sizeof(connID_t));
                    connid2proxy[connectionID]->send(buffer, sizeof(buffer));

                    if (h.isClosed() == std::make_pair(true, true)){
                        loc2connID.erase_key(connId);
                        connid2proxy.erase(connectionID);
                        id2handle.erase(connId);
                    }
                }
                
                const auto& dest = proc2proc.find(connId);
                if (dest != proc2proc.end()){
                    id2handle[dest->second].close();

                    if (id2handle[dest->second].isClosed() == std::make_pair(true, true)){
                        proc2proc.erase(dest->second);
                        id2handle.erase(dest->second);
                    }
                }

                continue;
            }
            char* buffer = new char[sizeof(cmd_t)+sizeof(connID_t)+sz];
            h.receive(buffer+sizeof(cmd_t)+sizeof(connID_t), sz);

            if (loc2connID.has_key(connId)){
                buffer[0] = cmd_t::FWD;
                connID_t connectionID = loc2connID.get_value(connId);
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
        }
    }

    Manager::finalize();
    return 0;
}
