ifndef CXX
CXX 	   = g++
endif

MTCL_DIR=../..

CXXFLAGS  += -std=c++17 -DENABLE_CONFIGFILE
INCS       = -I . -I $(MTCL_DIR)/include

ifdef DEBUG
	OPTIMIZE_FLAGS  += -g -fno-inline-functions
else
	OPTIMIZE_FLAGS  += -O3 -finline-functions -DNDEBUG
endif

ifdef SINGLE_IO_THREAD
	CXXFLAGS +=-DSINGLE_IO_THREAD
endif

ifdef TPROTOCOL
ifndef RAPIDJSON_HOME
$(error RAPIDJSON_HOME env variable not defined!);
endif
endif

ifeq ($(findstring MPI,$(TPROTOCOL)),MPI)
	CXX 	  = mpicxx
	CXXFLAGS += -DENABLE_$(TPROTOCOL)
	TARGET    = $(MTCL_DIR)protocols/stop_accept
ifdef MPI_HOME
	INCS   += `pkg-config --cflags-only-I $(MPI_HOME)/lib/pkgconfig/ompi-cxx.pc`
	LIBS   += `pkg-config --libs $(MPI_HOME)/lib/pkgconfig/ompi-cxx.pc`
endif
endif

ifeq ($(findstring MQTT, $(TPROTOCOL)),MQTT)
	CXXFLAGS += -DENABLE_MQTT
ifndef PAHO_HOME
$(error PAHO_HOME env variable not defined!);
endif
	INCS += -I${PAHO_HOME}/include
	LIBS += -L${PAHO_HOME}/lib -Wl,-rpath,${PAHO_HOME}/lib -lpaho-mqttpp3 -lpaho-mqtt3as -lpaho-mqtt3a
endif

ifeq ($(findstring TCP, $(TPROTOCOL)),TCP)
	CXXFLAGS += -DENABLE_TCP
endif

ifeq ($(findstring UCX, $(TPROTOCOL)),UCX)
ifndef UCC_HOME
$(error UCC_HOME env variable not defined!);
endif
	CXXFLAGS += -DENABLE_UCX
ifndef UCX_HOME
$(error UCX_HOME env variable not defined!);
endif
	INCS += -I$(UCX_HOME)/include -I$(UCC_HOME)/include
	LIBS += -L$(UCX_HOME)/lib -Wl,-rpath,${UCX_HOME}/lib -lucp -luct -lucs -lucm -L${UCC_HOME}/lib -Wl,-rpath,${UCC_HOME}/lib -lucc
endif

CXXFLAGS         += -Wall
LIBS             += -I ${RAPIDJSON_HOME}/include -pthread -lrt
INCLUDES          = $(INCS)

SOURCES           = $(wildcard *.cpp)
TARGET           += $(SOURCES:.cpp=)

.PHONY: all clean cleanall 
.SUFFIXES: .c .cpp .o

%.d: %.cpp
	@set -e; $(CXX) -MM $(INCLUDES) $(CXXFLAGS) $< \
		| sed 's/\($*\)\.o[ :]*/\1 $@ : /g' > $@; \
		[ -s $@ ] || rm -f $@
%.d: %.c
	@set -e; $(CC) -MM $(INCLUDES) $(CFLAGS)  $< \
		| sed 's/\($*\)\.o[ :]*/\1 $@ : /g' > $@; \
		[ -s $@ ] || rm -f $@
%.o: %.c
	$(CC) $(INCLUDES) $(CFLAGS) -c -o $@ $<
%: %.cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) $(OPTIMIZE_FLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

all: $(TARGET)

clean: 
	-rm -fr $(TARGET) *~
cleanall: clean
	-rm -fr *.d uri_file.txt $(MTCL_DIR)/protocols/stop_accept

include $(SOURCES:.cpp=.d)
