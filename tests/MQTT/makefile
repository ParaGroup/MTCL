CXX 		= g++
CXXFLAGS 	= -g -Wall -std=c++17 -DENABLE_MQTT 

LIBS 		= -L${PAHO_HOME}/lib -Wl,-rpath,${PAHO_HOME}/lib -lpaho-mqttpp3 -lpaho-mqtt3as -lpaho-mqtt3a -lpthread -lpaho-mqtt3a -lpaho-mqttpp3

INCLUDES 	= -I ../../include -I${PAHO_HOME}/include

TARGETS = mqtt_test

.PHONY = all clean cleanall

all: $(TARGETS)

%: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $< -o $@ $(LIBS)

clean:
	rm -f $(TARGETS)

cleanall: clean
	rm -f ./*.out ./*.o ./*~
