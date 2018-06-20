# Terra
This is the Terra/Gaia framework for making scheduling decisions and transferring the data using multipath.

This framework is dependent on gaia-lib.

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
java -cp java -cp target/gaia_all-jar-with-dependencies.jar gaiaframework.MasterApp -g <gml> -c <conf> -e -s gaia
```