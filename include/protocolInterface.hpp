#ifndef PROTOCOLINTERFACE_HPP
#define PROTOCOLINTERFACE_HPP

#include <queue>
#include <mutex>
#include <functional>
#include <errno.h>

class Handle;
class ConnType {

    friend class Manager;
    friend class Handle;
    std::string instanceName;
protected:

    std::function<void(bool, Handle*)> addinQ;

    static void setAsClosed(Handle* h, bool blockflag);

public:
    ConnType() {};
    virtual ~ConnType() {};
    
    /**
     * @brief Initialize the ConnType object.
     * 
     * @return \c 0 if success, error code otherwise
     */
    virtual int init(std::string) = 0;


    /**
     * @brief Start listening for new connections at the specified address \b s.
     * 
     * @param[in] s listening address for this object
     * @return 0 if success, error code otherwise
     */
    virtual int listen(std::string s) = 0;


    /**
     * @brief Create a new Handle object establishing a connection to the remote
     * peer specified by string \b s.
     * 
     * @param[in] s address of the peer to which the connection is to be established
     * @return Pointer to the connected Handle object
     */
    virtual Handle* connect(const std::string& s, int retry, unsigned timeout) = 0;


    /**
     * @brief Non-blocking polling over the managed set of Handle objects. It
     * should be used to check whether Handles are ready to receive a message
     * or they received a connection close event from the remote peer.
     * Must return control to the Manager after the polling phase is completed.
     * 
     */
    virtual void update() = 0; 
    

    /**
     * @brief Manage the notification of an Handle object \b h issuing a yield operation.
     * After this method is complete, the ownership of \b h
     * is transferred back to the parent ConnType.
     * 
     * @param[in] h Handle yielding control to the parent ConnType object
     */
    virtual void notify_yield(Handle* h) = 0;
    

    /**
     * @brief Manage the notification of an Handle object \b h issuing a close operation.
     * The Handle is invalidated and should not be managed anymore.
     * 
     * @param[in] h Handle closing the connection
     */
    virtual void notify_close(Handle* h, bool shut_wr=true, bool shut_rd=true) = 0;

    /**
     * @brief Stop listening for new connections and terminate existing Handle\a s.
     * 
     */
    virtual void end(bool blockflag=false) = 0;
};

#endif
