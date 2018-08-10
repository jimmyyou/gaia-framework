package gaiaframework.gaiaagent;

// New sending agent using grpc.

import gaiaframework.network.NetGraph;
import gaiaframework.util.Configuration;

import gaiaframework.util.Constants;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class SendingAgent {
    private static final Logger logger = LogManager.getLogger();
    protected static Configuration config;
    private static ScheduledExecutorService statusReportExec;

    public static void main(String[] args) {
        Options options = new Options();
        options.addRequiredOption("g", "gml",true, "path to gml file");
        options.addRequiredOption("i", "id" ,true, "ID of this sending agent");
        options.addOption("c", "config",true, "path to config file");
        options.addOption("n" , "total-number" , true , "total number of agents");

        String said = null;
        String configfilePath;
        String gmlFilePath = null;

        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();

        if(args.length == 0) {
            formatter.printHelp( "SendingAgent -i [id] -g [gml] -c [config]", options );
            return;
        }

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("i")){
                said = cmd.getOptionValue('i');
            }

            if (cmd.hasOption("c")){
                configfilePath = cmd.getOptionValue('c');
                logger.info("using configuration from " + configfilePath);
                config = new Configuration(configfilePath);
            }
            else {
                if (cmd.hasOption('n')){
                    logger.info("using default configuration.");
                    config = new Configuration(Integer.parseInt(cmd.getOptionValue('n')));
                }
                else {
                    logger.error("Can't use default configuration without -n option.");
                    System.exit(1);
                }
            }

            if (cmd.hasOption("g")){
                gmlFilePath = cmd.getOptionValue('g');
                logger.info("using gml from file: " + gmlFilePath);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

        NetGraph net_graph = null; // sending agent is unaware of the bw_factor
        try {
            net_graph = new NetGraph(gmlFilePath ,1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        AgentSharedData sharedData;
        sharedData = new AgentSharedData(said, net_graph);

        final AgentRPCServer server = new AgentRPCServer(said, net_graph, config, sharedData);

        Thread fumListener = new Thread( new CTRLMsgListenerThread(sharedData.fumQueue, sharedData));
        fumListener.start();

        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // wait for the status become READY, then start the Client
        logger.info("Waiting for the READY status before starting the gRPC client");

        try {
            sharedData.readySignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.info("agent now READY, starting agent RPC client");
        // start the client first? no! start the client after the server
        sharedData.rpcClient = new AgentRPCClient(config.getMasterIP(), config.getMasterPort(), sharedData);

        statusReportExec = Executors.newScheduledThreadPool(1);

//        final Runnable sendStatus = () -> sharedData.rpcClient.sendStatusUpdate();
        final Runnable sendStatus = () -> sharedData.pushStatusUpdate();

//        (new Thread ( new CTRLMsgSenderThread(sharedData)) ).start();

        ScheduledFuture<?> mainHandler = statusReportExec.scheduleAtFixedRate(sendStatus, 0, Constants.STATUS_MESSAGE_INTERVAL_MS, MILLISECONDS);

        try {
            server.blockUntilShutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}
