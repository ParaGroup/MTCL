
## MTCL Proxy

In order to use the proxy you should compile and run it before executing the final application. All the proxies must have the very same configuration file of the application you are going to run.


Proxy to proxy available protocols are: **TCP** and **MQTT**.

In order to use MQTT as a PROXY-to-PROXY protocol compile with the following macros.

> TPROTOCOL=MQTT PPPROTOCOL=MQTT

Once MQTT is selected as PROXY-to-PROXY transport, it **cannot** be use between Proxy and applications.