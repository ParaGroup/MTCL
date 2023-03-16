#ifndef CONFIG_HPP
#define CONFIG_HPP


// -------------- some configuration parameters ----------------

// all timeouts are in microseconds unless otherwise stated
const unsigned IO_THREAD_POLL_TIMEOUT  = 10; 

// ------ TCP ------
const unsigned TCP_BACKLOG             = 128;
const unsigned TCP_POLL_TIMEOUT        = 10; 
const unsigned UNREACHABLE_ADDR_TIMOUT = 100;  // milliseconds   

// ------ SHM ------
const unsigned SHM_SMALL_MSG_SIZE      = (1<<22);
const unsigned SHM_MAX_CONCURRENT_CONN = 1024;

// ------ MPI ------
const unsigned MPI_POLL_TIMEOUT        = 10; 
const unsigned MPI_CONNECTION_TAG      = 0;
const unsigned MPI_DISCONNECT_TAG      = 1;

// ------ MPIP2P ------
const unsigned MPIP2P_POLL_TIMEOUT     = 10; 
const char MPIP2P_STOP_PROCESS[]       = "stop_accept";

// ------- MQTT -----
const unsigned MQTT_POLL_TIMEOUT       = 10;   // milliseconds
const unsigned MQTT_CONNECT_TIMEOUT    = 100;  // milliseconds
const std::string MQTT_OUT_SUFFIX{"-out"};
const std::string MQTT_IN_SUFFIX{"-in"};
const std::string MQTT_MANAGER_PSWD{"manager_passwd"};
const std::string MQTT_CONNECTION_TOPIC{"-new_connection"};
const std::string MQTT_EXIT_TOPIC {"-exit"};
// default broker address if not otherwise provided
const std::string MQTT_SERVER_ADDRESS{ "tcp://localhost:1883" };

// ------- UCX ------
const unsigned UCX_BACKLOG             = 128;
const unsigned UCX_POLL_TIMEOUT        = 10; 

// -------- COLLECTIVES ------
const int CCONNECTION_RETRY            = 10;
const unsigned CCONNECTION_TIMEOUT     = 100;  // milliseconds


#endif 
