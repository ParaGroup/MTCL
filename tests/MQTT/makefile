CXX 		= g++
CXXFLAGS 	= -g -Wall -std=c++17

LDLIBS 		= -lpthread -lpaho-mqtt3a -lpaho-mqttpp3

LIB_HOME 	= ../../

TARGETS = mqtt_test.out
DEPS = $(LIB_HOME)manager.hpp $(LIB_HOME)protocolInterface.hpp $(LIB_HOME)handleUser.hpp $(LIB_HOME)handle.hpp $(LIB_HOME)protocols/mqtt.hpp

.PHONY = all clean cleanall

all: $(TARGETS)

simple_test: mqtt_test.out

%.out: %.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDLIBS) -I $(LIB_HOME)

clean:
	rm -f $(TARGETS)

cleanall: clean
	rm -f ./*.out ./*.o ./*~
