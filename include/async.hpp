#ifndef MTCL_ASYNC_HPP
#define MTCL_ASYNC_HPP

namespace MTCL {

class request_internal {
public:
    virtual int test(int& result) = 0;
    virtual ~request_internal() {} // make sure to delete the whole inherited object
};

class dummy_request_internal : public request_internal {
    int test(int& result){result = 1; return 0;}
};

class Request {

    template <typename... R> friend bool testAll(const Request&...);
    template <typename... R> friend void waitAll(const Request&, const R&...);
    request_internal* r;

    // disable copy constructor and assignment
    Request(const Request&);
    Request& operator=(const Request&);


public:
    // allow just move constructor and move assignment
    Request(Request&&) = default;
    Request& operator=(Request&& i){
        r = i.r;
        i.r = nullptr;
        return *this;
    }

    Request() : r(new dummy_request_internal) {}
    Request(request_internal* r) : r(r) {}
    ~Request(){
        if (r) delete r; // it should not be called after the object is moved
    }
};


template <typename... Request>
bool testAll(const Request&... requests) {
    int outTest = false;
    for(const auto& p : {requests...}) {
        p.r->test(&outTest);
        if (!outTest) return false; 
    }
    return true;
}

template <typename... Args>
void waitAll(const Request& f, const Args&... fs){
    int outTest = 0;
    while(true){
        bool allCompleted = true;
        for(auto p : {&f, &fs...}) {
            p->r->test(outTest);
            if (!outTest) allCompleted = false; 
        }
        if (allCompleted) return;
        sleep(1);
    }
}



}

#endif