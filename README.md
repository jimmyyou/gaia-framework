# Terra
This is the Terra/Gaia framework for making scheduling decisions and transferring the data using multipath.

This framework is dependent on gaia-lib.

## Requirements

`gaia-framework` needs to be built in Ubuntu 16.04 with Java version 8.

## How to build gaia-framework

To build `gaia-framework`, you need to first install `gaia-lib`. After that, run:

``mvn clean package``


## How to run gaia-framework

To start *Forwarding Agents*:

```
java -cp target/gaia_all-jar-with-dependencies.jar gaiaframework.transmission.ForwardingAgent -c <conf> -i <id>

```

To start *Receiving Agents*:

```
java -cp java -cp target/gaia_all-jar-with-dependencies.jar gaiaframework.receiver.ReceivingAgent [-p <port>]
```

To start *Sending Agents*:

```
java -cp target/gaia_all-jar-with-dependencies.jar gaiaframework.gaiaagent.SendingAgent -c <conf> -g <gml> -i <id>
```

To start *HTTP File Server*:
```
java -cp target/gaia_all-jar-with-dependencies.jar gaiaframework.HTTPServer.HttpStaticFileServer
```

To start *Master*:

```
java -cp java -cp target/gaia_all-jar-with-dependencies.jar gaiaframework.TerraMaster -g <gml> -c <conf> -e -s gaia
```

## How to config Terra
Using *.conf file we can config the IP:port for Terra Masters and Agents


Format:

```
<Master IP> <Master Port>
<Number of DCs>
{ // for each DC
    <DC_ID> <Number of Hosts>
    { // for each host
        <Sending Agent> <port>
        <Forwarding Agent> <port>
        <Receiving Agent> <port>
    }
}
```
 
