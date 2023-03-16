#ifndef MANAGER_HPP
#define MANAGER_HPP

#include <csignal>
#include <cstdlib>
#include <map>
#include <set>
#include <vector>
#include <queue>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <sstream>

#include "handle.hpp"
#include "handleUser.hpp"
#include "protocolInterface.hpp"
#include "protocols/tcp.hpp"
#include "protocols/shm.hpp"

#ifdef ENABLE_CONFIGFILE
#include <fstream>
#include <rapidjson/rapidjson.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/document.h>
#include "collectives/collectiveContext.hpp"
#endif

#ifdef ENABLE_MPI
#include "protocols/mpi.hpp"
#endif

#ifdef ENABLE_MPIP2P
#include "protocols/mpip2p.hpp"
#endif

#ifdef ENABLE_MQTT
#include "protocols/mqtt.hpp"
#endif

#ifdef ENABLE_UCX
#include "protocols/ucx.hpp"
#endif

int  mtcl_verbose = -1;

/**
 * Main class for the library
*/
class Manager {
    friend class ConnType;
    friend class CollectiveContext;
   
    inline static std::map<std::string, std::shared_ptr<ConnType>> protocolsMap;    
	inline static std::queue<HandleUser> handleReady;
    inline static std::map<std::string, std::vector<Handle*>> groupsReady;
    inline static std::map<CollectiveContext*, bool> contexts;
    inline static std::set<std::string> listening_endps;
	inline static std::set<std::string> createdTeams;
	
    inline static std::string appName;
    inline static std::string poolName;

#ifdef ENABLE_CONFIGFILE
    inline static std::map<std::string, std::pair<std::vector<std::string>, std::vector<std::string>>> pools;
    inline static std::map<std::string, std::tuple<std::string, std::vector<std::string>, std::vector<std::string>>> components;
#endif

    REMOVE_CODE_IF(inline static std::thread t1);
    inline static bool end;
    inline static bool initialized = false;

    inline static std::mutex mutex;
    inline static std::mutex group_mutex;
    inline static std::mutex ctx_mutex;
    inline static std::condition_variable condv;
    inline static std::condition_variable group_cond;

private:
    Manager() {}

	// initial handshake for a connection, it could be a p2p connection or a connection
	// part of a collective handle
	static inline int connectionHandshake(char *& teamID, Handle *h) {
		// new connection, read handle type (p2p=0, collective=1)
		size_t size;
		if (h->probe(size, true) <=0) {
			MTCL_ERROR("[Manager]:\t", "addinQ handshake error in probe, errno=%d\n", errno);
			teamID=nullptr;
			return -1;
		}
		int collective = 0;		
		if (h->receive(&collective, sizeof(int)) <=0) {
			MTCL_ERROR("[Manager]:\t", "addinQ handshake error in receiving collective flag, errno=%d\n", errno);
			teamID=nullptr;
			return -1;
		}
        
		// If collective, the handle sends further data with string teamID.
		// The teamID uniquely associate a single context to all handles of the same collective
		// This is useful to synchronize the root thread for the accepts.
		if(collective) {
			size_t size;
			if (h->probe(size, true) <= 0) {
				MTCL_ERROR("[Manager]:\t", "addinQ handshake error in probe, teamID size, errno=%d\n", errno);
				teamID=nullptr;
				return -1;
			}
			// sanity check
			if (size>1048576) {
				MTCL_ERROR("[Manager]:\t", "addinQ handshake error in probe, teamID size TOO LARGE (size=%ld)\n", size);
				teamID=nullptr;
				return -1;
			}
			
			teamID = new char[size+1];
			assert(teamID);
			if (h->receive(teamID, size) <=0) {
				MTCL_ERROR("[Manager]:\t", "addinQ handshake error in probe, receiving teamID, errno=%d\n", errno);
				delete [] teamID;
				teamID=nullptr;
				return -1;
			}
			teamID[size] = '\0';			
			MTCL_PRINT(100, "[Manager]: \t", "Manager::addinQ received connection for team: %s\n", teamID);
		}		
		return 0;
	}
	
#if defined(SINGLE_IO_THREAD)
	static inline void addinQ(bool b, Handle* h) {
        if(b) { // we have to see if it is part of a collective
			char *teamID=nullptr;
			if (connectionHandshake(teamID, h)==-1) return;

			if (teamID) {
                if(groupsReady.count(teamID) == 0)
                    groupsReady.emplace(teamID, std::vector<Handle*>{});
				
                groupsReady.at(teamID).push_back(h);
                delete[] teamID;
                return;
            }
        }
		
		handleReady.push(HandleUser(h, true, b));
	}
#else	
    static inline void addinQ(const bool b, Handle* h) {

        if(b) { // For each new connection... is the handle coming from a collective?
			char *teamID = nullptr;
			if (connectionHandshake(teamID, h) == -1) return;

			if (teamID) {
				std::unique_lock lk(group_mutex);
                if(groupsReady.count(teamID) == 0)
                    groupsReady.emplace(teamID, std::vector<Handle*>{});
				
                groupsReady.at(teamID).push_back(h);
                delete[] teamID;
                group_cond.notify_one();
                return;
            }
        }

        std::unique_lock lk(mutex);
        handleReady.push(HandleUser(h, true, b));
		condv.notify_one();
    }
#endif
	
    static bool poll(CollectiveContext* realHandle) {
		if (realHandle->probed.first) { // previously probed
            return true;
		}

        return realHandle->peek();
    }

	// IO thread function
    static void getReadyBackend() {
        while(!end){
            for(auto& [prot, conn] : protocolsMap) {
                conn->update();
            }			
			if constexpr (IO_THREAD_POLL_TIMEOUT)
				std::this_thread::sleep_for(std::chrono::microseconds(IO_THREAD_POLL_TIMEOUT));

            {
                std::unique_lock lk(ctx_mutex);
                for(auto& [ctx, toManage] : contexts) {
                    if(toManage) {
                        bool res = poll(ctx);
                        if(res) {
                            toManage = false;
                            std::unique_lock readylk(mutex);
                            handleReady.push(HandleUser(ctx, true, false));
                            condv.notify_one();
                        }
                    }
                }
            }
        }
    }
#ifdef ENABLE_CONFIGFILE
    template <bool B, typename T>
    static std::vector<std::string> JSONArray2VectorString(const rapidjson::GenericArray<B, T>& arr){
        std::vector<std::string> output;
        for(auto& e : arr)  
            output.push_back(e.GetString());
        
        return output;
    }
    
    static int parseConfig(std::string& f){
        std::ifstream ifs(f);
        if ( !ifs.is_open() ) {
			MTCL_ERROR("[Manager]:\t", "parseConfig: cannot open file %s for reading, skip it\n",
					   f.c_str());
            return -1;
        }
        rapidjson::IStreamWrapper isw { ifs };
        rapidjson::Document doc;
        doc.ParseStream( isw );
        if(doc.HasParseError()) {
            MTCL_ERROR("[internal]:\t", "Manager::parseConfig JSON syntax error in file %s\n", f.c_str());
			return -1;
        }

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
#endif


    static void releaseTeam(CollectiveContext* ctx) {
        std::unique_lock lk(ctx_mutex);
        auto it = contexts.find(ctx);
        if (it != contexts.end())
            it->second = true;
    }


public:

    /**
     * \brief Initialization of the manager. Required to use the library
     * 
     * Internally this call creates the backend thread that performs the polling over all registered protocols.
     * 
     * @param appName Application ID for this application instance
     * @param configFile (Optional) Path of the configuration file for the application. It can be a unique configuration file containing both architecture information and application specific information (deployment included).
     * @param configFile2 (Optional) Additional configuration file in the case architecture information and application information are splitted in two separate files. 
    */
    static int init(std::string appName, std::string configFile1 = "", std::string configFile2 = "") {
		std::signal(SIGPIPE, SIG_IGN);

		char *level;
		if ((level=std::getenv("MTCL_VERBOSE"))!= NULL) {
			if (std::string(level)=="all" ||
				std::string(level)=="ALL" ||
				std::string(level)=="max" ||
				std::string(level)=="MAX")
				mtcl_verbose = std::numeric_limits<int>::max(); // print everything
			else
				try {
					mtcl_verbose=std::stoi(level);
					if (mtcl_verbose<=0) mtcl_verbose=1;
				} catch(...) {
					MTCL_ERROR("[Manger]:\t", "invalid MTCL_VERBOSE value, it should be a number or all|ALL|max|MAX\n");
				}
		}
		
        Manager::appName = appName;

		// default transports protocol
        registerType<ConnTcp>("TCP");

		registerType<ConnSHM>("SHM");

#ifdef ENABLE_MPI
        registerType<ConnMPI>("MPI");
#endif

#ifdef ENABLE_MPIP2P
        registerType<ConnMPIP2P>("MPIP2P");
#endif

#ifdef ENABLE_MQTT
        registerType<ConnMQTT>("MQTT");
#endif

#ifdef ENABLE_UCX
        registerType<ConnUCX>("UCX");
#endif

#ifdef ENABLE_CONFIGFILE
        if (!configFile1.empty()) if (parseConfig(configFile1)<0) return -1;
        if (!configFile2.empty()) if (parseConfig(configFile2)<0) return -1;

        // if the current appname is not found in configuration file, abort the execution.
        if (components.find(appName) == components.end()){			
            MTCL_ERROR("[Manager]", "Component %s not found in configuration file\n", appName.c_str());
            return -1;
        }

        // set the pool name if in the host definition
        poolName = getPoolFromHost(std::get<0>(components[appName]));

#else
     // 
#endif
		end = false;
        for (auto &el : protocolsMap) {
            if (el.second->init(appName) == -1) {
				MTCL_PRINT(100, "[Manager]:\t", "ERROR initializing protocol %s\n", el.first.c_str());
			}
        }
#ifdef ENABLE_CONFIGFILE
        // Automatically listen from endpoints listed in config file
        for(auto& le : std::get<2>(components[Manager::appName])){
            Manager::listen(le);
        }
#endif

        REMOVE_CODE_IF(t1 = std::thread([&](){Manager::getReadyBackend();}));

        initialized = true;
		return 0;
    }

    /**
     * \brief Finalize the manger closing all the pending open connections.
     * 
     * From this point on, no more interactions with the library and the manager should be done. 
	 * Ideally this call must be invoked just before closing the application (return statement of 
	 * the main function).
     * Internally it stops the polling thread started at the initialization and calls the end 
	 * method of each registered protocols.
	 */
    static void finalize(bool blockflag=false) {
		end = true;
        REMOVE_CODE_IF(t1.join());

        //while(!handleReady.empty()) handleReady.pop();

        for(auto& [ctx, _] : contexts) {
            ctx->finalize(blockflag, ctx->getName());
            if(ctx->counter == 1)
                delete ctx;
            else ctx->counter--;
        }

        for (auto [_,v]: protocolsMap) {
            v->end(blockflag);
        }

    }

    /**
     * \brief Get an handle ready to receive.
     * 
     * The returned value is an Handle passed by value.
    */  
#if defined(SINGLE_IO_THREAD)
    static inline HandleUser getNext(std::chrono::microseconds us=std::chrono::hours(87600)) {
		if (!handleReady.empty()) {
			auto el = std::move(handleReady.front());
			handleReady.pop();
			return el;
		}
		// if us is not multiple of the IO_THREAD_POLL_TIMEOUT we wait a bit less....
		// if the poll timeout is 0, we just iterate us times
		size_t niter = us.count(); // in case IO_THREAD_POLL_TIMEOUT is set to 0
		if constexpr (IO_THREAD_POLL_TIMEOUT)
			niter = us/std::chrono::milliseconds(IO_THREAD_POLL_TIMEOUT);
		if (niter==0) niter++;
		size_t i=0;
		do { 
			for(auto& [prot, conn] : protocolsMap) {
				conn->update();
			}

            for(auto& [ctx, toManage] : contexts) {
                if(toManage) {
                    bool res = poll(ctx);
                    if(res) {
                        toManage = false;
                        handleReady.push(HandleUser(ctx, true, false));
                    }
                }
            }

			if (!handleReady.empty()) {
				auto el = std::move(handleReady.front());
				handleReady.pop();
				return el;
			}
			if (i >= niter) break;
			++i;
			if constexpr (IO_THREAD_POLL_TIMEOUT)
				std::this_thread::sleep_for(std::chrono::microseconds(IO_THREAD_POLL_TIMEOUT));	
		} while(true);
		return HandleUser(nullptr, true, true);
    }	
#else	
    static inline HandleUser getNext(std::chrono::microseconds us=std::chrono::hours(87600)) { 
        std::unique_lock lk(mutex);
        if (condv.wait_for(lk, us, [&]{return !handleReady.empty();})) {
			auto el = std::move(handleReady.front());
			handleReady.pop();
			lk.unlock();
			return el;
		}
        return HandleUser(nullptr, true, true);
    }
#endif
	
    /**
     * \brief Create an instance of the protocol implementation.
     * 
     * @tparam T class representing the implementation of the protocol being register
     * @param name string representing the name of the instance of the protocol
    */
    template<typename T>
    static void registerType(std::string name){
        static_assert(std::is_base_of<ConnType,T>::value, "Not a ConnType subclass");
        if(initialized) {
			MTCL_ERROR("[Manager]:\t", "The Manager has been already initialized. Impossible to register new protocols.\n");
            return;
        }

        protocolsMap[name] = std::shared_ptr<T>(new T);
        
        protocolsMap[name]->addinQ = [&](bool b, Handle* h){ Manager::addinQ(b,h); };

        protocolsMap[name]->instanceName = name;
    }

    /**
     * \brief Listen on incoming connections.
     * 
     * Perform the listen operation on a particular protocol and parameters given by the string passed as parameter.
     * 
     * @param connectionString URI containing parameters to perform the effective listen operation
    */
    static int listen(std::string s) {
        // Check if Manager has already this endpoint listening
        if(listening_endps.count(s) != 0) return 0;
        listening_endps.insert(s);

        std::string protocol = s.substr(0, s.find(":"));
        
        if (!protocolsMap.count(protocol)){
			errno=EPROTO;
            return -1;
        }
        return protocolsMap[protocol]->listen(s.substr(protocol.length()+1, s.length()));
    }


    static Handle* connectHandle(std::string s, int retry, unsigned timeout) {
        size_t pos;
        std::string protocol = s.substr(0, (pos = s.find(":")) == std::string::npos ? 0 : pos);
       
        if(protocol.empty()){
            // checking if there is a config file to cycle on all addresses
#ifdef ENABLE_CONFIGFILE
            if (components.count(s))
                for(auto& le : std::get<2>(components[s])){
                    std::string sWoProtocol = le.substr(le.find(":") + 1, le.length());
                    std::string remote_protocol = le.substr(0, le.find(":"));
                    if (protocolsMap.count(remote_protocol)){
                        auto* h = protocolsMap[remote_protocol]->connect(sWoProtocol, retry, timeout);
                        if (h) return h;
                    }
                }
#endif
            MTCL_ERROR("[internal]:\t", "Manager::connectHandle specified appName (%s) not found in configuration file.\n", s.c_str());
            return nullptr;
        }

        // POSSIBILE LABEL
        std::string appLabel = s.substr(s.find(":") + 1, s.length());

        #ifdef ENABLE_CONFIGFILE
            if (components.count(appLabel)){ // there is a label
                auto& component = components.at(appLabel);
                std::string& host = std::get<0>(component); // host= [pool:]hostname
                std::string pool = getPoolFromHost(host);

                    if (pool != poolName){ 
                        std::string connectionString2Proxy;
                        if (poolName.empty() && !pool.empty()){ // go through the proxy of the destination pool
                            // connect verso il proxy di pool
                            if (protocol == "UCX" || protocol == "TCP"){
                               for (auto& ip: pools[pool].first){
                                // if the ip contains a port is better to skip it, probably is a tunnel used betweens proxies
                                Handle* handle;
				if (ip.find(":") != std::string::npos) 
					handle = protocolsMap["TCP"]->connect(ip, retry,timeout);
				else
					handle = protocolsMap[protocol]->connect(ip + ":" + (protocol == "UCX" ? "13001" : "13000"), retry, timeout);
                                //handle->send(s.c_str(), s.length());
                                if (handle){
					handle->send(s.c_str(), s.length());
				       	return handle;
				}
                               }
                            } else {
                                auto* handle = protocolsMap[protocol]->connect("PROXY-" + pool, retry, timeout);
                                handle->send(s.c_str(), s.length());
                                if (handle) return handle;
                            }
                        }
                    
                        if (!poolName.empty() && !pool.empty()){ // try to contact my proxy
                            if (protocol == "UCX" || protocol == "TCP"){
                               for (auto& ip: pools[poolName].first){
								   auto* handle = protocolsMap[protocol]->connect(ip + ":" + (protocol == "UCX" ? "13001" : "13000"), retry, timeout);
                                handle->send(s.c_str(), s.length());
                                if (handle) return handle;
                               }
                            } else {
                                auto* handle = protocolsMap[protocol]->connect("PROXY-" + poolName, retry, timeout);
                                handle->send(s.c_str(), s.length());
                                if (handle) return handle;
                            }
                        }
                        return nullptr;
                    } else {
                        // connessione diretta
                        for (auto& le : std::get<2>(component))
                            if (le.find(protocol) != std::string::npos){
                                auto* handle = protocolsMap[protocol]->connect(le.substr(le.find(":") + 1, le.length()), retry, timeout);
                                if (handle) return handle;
                            }
                        
                        return nullptr;
                    } 
                
  
            }
        #endif

        if(protocolsMap.count(protocol)) {
            Handle* handle = protocolsMap[protocol]->connect(s.substr(s.find(":") + 1, s.length()),
															 retry, timeout);
            if(handle) {
                return handle;
            }
        }

        return nullptr;
    }


    /*  Team: App1 App2 App3.
        Broadcast:
          App1(root) ---->  | App2
                            | App3

        Fan-out
            App1(root) --> | App2 or App3

        Fan-in
            App2 or App3 --> | App1(root)

        AllGather
            App1 --> |App2 and App3   ==  App2 & App3 --> | App1 (gather) --> | App2 & App3 (Broadcast result)
            App2 --> |App1 and App3
            App3 --> |App1 and App2
    */
    static HandleUser createTeam(const std::string participants, const std::string root, HandleType type) {


#ifndef ENABLE_CONFIGFILE
        MTCL_ERROR("[Manager]:\t", "Manager::createTeam team creation is only available with a configuration file\n");
        return HandleUser();
#else
        std::string teamID{participants + root + "-" + std::to_string(type)};

		if (createdTeams.count(teamID) != 0) {
			MTCL_ERROR("[Manager]:\t", "Manager::createTeam, team already created [%s]\n", teamID.c_str());
			errno=EINVAL;
			return HandleUser();
		}
		createdTeams.insert(teamID);
		
        // Retrieve team size
        int size = 0;
        std::istringstream is(participants);
        std::string line;
        int rank = 0;
        bool mpi_impl = true, ucc_impl = true;
        bool root_ok = false;

        while(std::getline(is, line, ':')) {
            if(Manager::appName == line) {
                rank=size;
            }
            if(root == line) root_ok=true;

            bool mpi = false;
            bool ucc = false;
            if(components.count(line) == 0) {
                MTCL_ERROR("[internal]:\t", "Manager::createTeam missing \"%s\" in configuration file\n", line.c_str());
				return HandleUser();
            }

            auto protocols = std::get<1>(components[line]);
            for (auto &prot : protocols) {
                mpi |= prot == "MPI";
                ucc |= prot == "UCX";
            }

            mpi_impl &= mpi;
            ucc_impl &= ucc;

            size++;
        }

        if(std::get<2>(components[root]).size() == 0) {
            MTCL_ERROR("[internal]:\t", "Manager::createTeam root App [\"%s\"] has no listening endpoints\n", root.c_str());
			return HandleUser();
        }

        if(!root_ok) {
            MTCL_ERROR("[internal]:\t", "Manager::createTeam missing root App [\"%s\"] in participants string\n", root.c_str());
			return HandleUser();
        }

        MTCL_PRINT(100, "[Manager]:\t", "Manager::createTeam initializing collective with size: %d - AppName: %s - rank: %d - mpi: %d - ucc: %d\n", size, Manager::appName.c_str(), rank, mpi_impl, ucc_impl);


        std::vector<Handle*> coll_handles;

        ImplementationType impl;
        if (mpi_impl) {
			impl = MPI;
			if constexpr (!MPI_ENABLED) {
					MTCL_ERROR("[Manager]:\t", "Manager::createTeam the selected protocol (MPI) has not been enabled AppName: %s\n", Manager::appName.c_str());
					return HandleUser();
			}
		} else if(ucc_impl) {
			impl = UCC;
			if constexpr (!UCC_ENABLED) {
					MTCL_ERROR("[Manager]:\t", "Manager::createTeam the selected protocol (UCX/UCC) has not been enabled AppName: %s\n", Manager::appName.c_str());
					return HandleUser();
				}
		}
        else impl = GENERIC;

        auto ctx = createContext(type, size, Manager::appName == root, rank);
        if(Manager::appName == root) {
            if(ctx == nullptr) {
                MTCL_ERROR("[Manager]:\t", "Operation type not supported\n");
                return HandleUser();
            }

#if defined(SINGLE_IO_THREAD)
                if ((groupsReady.count(teamID) != 0) && ctx->update(groupsReady.at(teamID).size())) {
                    coll_handles = groupsReady.at(teamID);
                    groupsReady.erase(teamID);
                }
                else {
                    //NOTE: Active and indefinite wait for group creation
                    do { 
                        for(auto& [prot, conn] : protocolsMap) {
                            conn->update();
                        }
                        if ((groupsReady.count(teamID) != 0) && ctx->update(groupsReady.at(teamID).size())) {
                            coll_handles = groupsReady.at(teamID);
                            for(auto h : coll_handles) {
                                h->setName(teamID+"-"+Manager::appName);
                            }
                            groupsReady.erase(teamID);
                            break;
                        }
                    } while(true);
                }
#else
                // Retrieving the connected handles associated to the collective
                std::unique_lock lk(group_mutex);
                group_cond.wait(lk, [&]{
                    return (groupsReady.count(teamID) != 0) && ctx->update(groupsReady.at(teamID).size());
                });
                coll_handles = groupsReady.at(teamID);

				for(auto h : coll_handles) {
					h->setName(teamID+"-"+Manager::appName);
				}
				
                groupsReady.erase(teamID);
                size--;
#endif
        }
        else {
            if(components.count(root) == 0) {
                MTCL_ERROR("[Manager]:\t", "Requested root node is not in configuration file\n");
                return HandleUser();
            }

            // Retrieve root listening addresses and connect to one of them
            auto root_addrs = std::get<2>(components.at(root));

            Handle* handle = nullptr;
            for(auto& addr : root_addrs) {
                //TODO: need to detect the protocol for the connect
                //      if mpi_impl/ucc_impl, then we must use the proper protocol
                handle = connectHandle(addr, CCONNECTION_RETRY, CCONNECTION_TIMEOUT);
                if(handle != nullptr) {
                    MTCL_PRINT(100, "[Manager]:\t", "Connection ok to %s\n", addr.c_str());
                    break; 
                }
                MTCL_PRINT(100, "[Manager]:\t", "Connection failed to %s\n", addr.c_str());
            }

            if(handle == nullptr) {
                MTCL_ERROR("[Manager]:\t", "Could not establish a connection with root node \"%s\"\n", root.c_str());
                return HandleUser();
            }
			
            int collective = 1;
            handle->send(&collective, sizeof(int));
            handle->send(teamID.c_str(), teamID.length());

			handle->setName(teamID+"-"+Manager::appName);

            coll_handles.push_back(handle);
        }
		std::hash<std::string> hashf;
		int uniqtag = static_cast<int>(hashf(teamID) % std::numeric_limits<int>::max());
        if(!ctx->setImplementation(impl, coll_handles, uniqtag)) {
            return HandleUser();
        }
        ctx->setName(teamID+"-"+Manager::appName);
		{
			std::unique_lock lk(ctx_mutex);
			contexts.emplace(ctx, false);
		}
        return HandleUser(ctx, true, true);

#endif

    }


    /**
     * \brief Connect to a peer
     * 
     * Connect to a peer following the URI passed in the connection string or following a label defined in the configuration file.
     * The URI is of the form "PROTOCOL:param:param: ... : param"
     * 
     * @param connectionString URI of the peer or label 
    */
    static HandleUser connect(std::string s, int nretry=-1, unsigned timeout=0) {
        Handle* handle = connectHandle(s, nretry, timeout);

        // if handle is connected, we perform the handshake
        if(handle) {
            int collective = 0; // no nbh conversion
            if (handle->send(&collective, sizeof(int))==-1) {
                MTCL_ERROR("[Manager]:\t", "handshake error, errno=%d (%s)\n",
						   errno, strerror(errno));
                return HandleUser();				
			}
        }
		
        return HandleUser(handle, true, true);
    };

    /**
     * \brief Given an handle return the name of the protocol instance given in phase of registration.
     * 
     * Example. If TCP implementation was registered as Manager::registerType<ConnTcp>("EX"), this function on handles produced by that kind of protocol instance will return "EX".
    */
    static std::string getTypeOfHandle(HandleUser& h){
        auto realHandle = (Handle*)h.realHandle;
        return realHandle->parent->instanceName;
    }

};

void CollectiveContext::yield() {
    if (!closed_rd && canReceive) {
        Manager::releaseTeam(this);
    }
    else if(!closed_rd) {
        MTCL_PRINT(1, "[internal]:\t", "CollectiveContext::yield cannot yield this context.\n");
        //TODO: if yield is called by HandleUser destructor, we may want to close
        //      the collective if its type is BROADCAST or GATHER
    }
}

#endif
