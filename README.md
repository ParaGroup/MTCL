
### Multi Transport Communication Library (MTCL)

The MTCL library aims to provide a common (and simple enough) interface for
several back-end communication libraries. Currently we support TCP/IP, MPI, MPIP2P (i.e., dynamic MPI), UCX, MQTT. The Shared Memory (SHM) support is still experimental.


### Dependencies

- ** MPI ** (OPTIONAL) :

  OpenMPI is required for using the MPI library as transport protocol.
  By default, it is disabled. It can be enabled by defining the
  environmental variable TPROTOCOL=MPI and by properly setting MPI_HOME
  to point to the OpenMPI install dir.

- ** MQTT ** (OPTIONAL) :

  For using the MQTT protocol as transport layer, Paho MQTT Cpp and
  a MQTT broker/server are needed.
  By default, MQTT is disabled. It can be enabled by defining the
  environmental variable ```TPROTOCOL=MQTT``` and by properly setting ```MQTT_HOME```
  to point to the Paho MQTT Cpp install dir.
  As MQTT broker/server Mosquitto is one of the options. Under Linux Ubuntu
  it can be installed using 'sudo apt install mosquitto'.

  --- How to compile and install Paho MQTT Cpp ----

  Download Paho MQTT C from github: https://github.com/eclipse/paho.mqtt.c

  ```
    export MQTT_HOME=<where-you-want-to-install-paho>
    cd paho.mqtt.c
    prefix=${MQTT_HOME} make install
  ```
  Download Paho MQTT Cpp from github: https://github.com/eclipse/paho.mqtt.cpp  

  ```
    cd paho.mqtt.cpp
    cmake -Bbuild -H. -DCMAKE_INSTALL_PREFIX=${MQTT_HOME} \
                      -DCMAKE_PREFIX_PATH=${MQTT_HOME}
    cd build
    make install
  ```

- ** UCX ** (OPTIONAL) :

    Download UCX from github: https://github.com/openucx/ucx

    --- How to compile and install UCX ---

    ```
    $ export UCX_HOME=<ucx-install-path>
    $ ./autogen.sh
    $ ./contrib/configure-release --prefix=${UCX_HOME} --enable-mt
    $ make -j8
    $ make install
    ```

- ** UCC ** (OPTIONAL, *requires UCX*):

    Download UCC from github: https://github.com/openucx/ucc

    --- How to compile and install UCC ---

    ```
    $ export UCC_HOME=<ucc-install-path>
    $ ./autogen.sh
    $ ./configure --prefix=${UCC_HOME} --with-ucx=<ucx-install-path> [--with-rocm=no --with-ibverbs=no --with-cuda=no]
    $ make
    ```

- ** RapidJSON **

  We use RapidJSON to parse configuration files. If you do not want
  to use the config file you don't need to install it.
  