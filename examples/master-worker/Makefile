ifndef CXX
CXX 	   = g++
endif
CXXFLAGS  += -std=c++17
INCS       = -I ../..
ifdef DEBUG
	OPTIMIZE_FLAGS  += -g -fno-inline-functions
else
	OPTIMIZE_FLAGS  += -O3 -finline-functions -DNDEBUG
endif
ifdef SINGLE_IO_THREAD
	CXXFLAGS +=-DSINGLE_IO_THREAD
endif

ifndef EXCLUDE_MPI
CXXFLAGS += -DENABLE_MPI
CXX 	  = mpicxx
ifdef MPI_HOME
	INCS   += `pkg-config --cflags-only-I $(MPI_HOME)/lib/pkgconfig/ompi-cxx.pc`
	LIBS   += `pkg-config --libs $(MPI_HOME)/lib/pkgconfig/ompi-cxx.pc`
endif
endif

ifndef EXCLUDE_UCX
	CXXFLAGS += -DENABLE_UCX
ifndef UCX_HOME
$(error UCX_HOME env variable not defined!);
endif
	INCS += -I$(UCX_HOME)/include
	LIBS += -L$(UCX_HOME)/lib -Wl,-rpath,${UCX_HOME}/lib -lucp -luct -lucs -lucm
endif

ifndef EXCLUDE_MQTT
	CXXFLAGS += -DENABLE_MQTT
ifndef PAHO_HOME
$(error PAHO_HOME env variable not defined!);
endif
	INCS += -I${PAHO_HOME}/include
	LIBS += -L${PAHO_HOME}/lib -Wl,-rpath,${PAHO_HOME}/lib -lpaho-mqttpp3 -lpaho-mqtt3as -lpaho-mqtt3a
endif


CXXFLAGS         += -Wall
LIBS             += -pthread -lrt
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
	-rm -fr *.d uri_file.txt

include $(SOURCES:.cpp=.d)
