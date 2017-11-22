package gaiaframework.transmission;

// Forwards the data to the specific destination host inside the DataCenter
// Each Data Center only has one ForwardingAgent

import gaiaframework.util.Configuration;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class ForwardingAgent {

    private static final Logger logger = LogManager.getLogger();

    String configfilePath;
    Configuration config;
    boolean isForwarding = true;
    int port = 33330;

    int faid;

    List<LinkedBlockingQueue<DataChunkMessage>> forwardingQueues;

    public ForwardingAgent(boolean isForwarding, int faID, String configFilePath) {
        this.isForwarding = isForwarding;
        this.faid = faID;
        this.configfilePath = configFilePath;

        this.config = new Configuration(configFilePath);
        this.port = config.getFAPort(faID);
    }

    public static void main(String[] args) {

        boolean isForwarding = true;
//        int port = 33330;
        int faID = -1;
        String configFilePath = null;

        Options options = new Options();
        options.addRequiredOption("c", "config", true, "path to config file");
        options.addRequiredOption("i", "id", true, "ID of this sending agent");
        options.addOption("n", "no-forward", false, "only receives, do not forward");

        HelpFormatter formatter = new HelpFormatter();

        if (args.length == 0) {
            formatter.printHelp("SendingAgent -c [config] -n", options);
            return;
        }

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("n")) {
                logger.info("Disabling forwarding");
                isForwarding = false;
            }

            if (cmd.hasOption("c")) {
                configFilePath = cmd.getOptionValue('c');
                logger.info("using configuration from " + configFilePath);
//                config = new Configuration(configFilePath);
            }

            if (cmd.hasOption("i")) {
                faID = Integer.parseInt(cmd.getOptionValue('i'));
            } else {
                logger.error("Must specify -i");
                System.exit(1);
            }

            ForwardingAgent fa = new ForwardingAgent(isForwarding, faID, configFilePath);
            fa.start();

        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    // main body
    private void start() {

        startClients();
        startServerAndBlock();

    }

    private void startServerAndBlock() {
        ServerSocket sd;
        int conn_cnt = 0;

        try {
            sd = new ServerSocket(port);
            sd.setReceiveBufferSize(64 * 1024 * 1024);
//            System.err.println("DEBUG, serversocket buffer: " + sd.getReceiveBufferSize());

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    try {
                        sd.close();
                        System.out.println("The server is shutting down");
                    } catch (IOException e) {
                        System.exit(1);
                    }
                }
            });


            while (true) {
                Socket dataSocket = sd.accept();
//                dataSocket.setSoTimeout(Constants.DEFAULT_SOCKET_TIMEOUT);
                conn_cnt++;
//                dataSocket.setSendBufferSize(16*1024*1024);
                dataSocket.setReceiveBufferSize(64 * 1024 * 1024);
                System.err.println("dataSoc buf " + dataSocket.getReceiveBufferSize());
//                dataSocket.setSoTimeout(0);
//                dataSocket.setKeepAlive(true);
                logger.info("{} Got a connection from {}", conn_cnt, dataSocket.getRemoteSocketAddress().toString());
                (new Thread(new ForwardingServer(config.getHostIPbyDCID(faid), dataSocket, forwardingQueues))).start();
            }
        } catch (java.io.IOException e) {
            e.printStackTrace();
//            System.exit(1);
        }
    }

    private void startClients() {

        int numHost = config.getNumHostbyDCID(faid);

        forwardingQueues = new ArrayList<LinkedBlockingQueue<DataChunkMessage>>(numHost);

        for (int i = 0; i < numHost; i++) {

            LinkedBlockingQueue<DataChunkMessage> queue = new LinkedBlockingQueue<>();

            forwardingQueues.add(queue);

            (new Thread(new BestEffortForwardingThread(queue, config.getHostIPbyDCID(faid).get(i), config.getHostPortbyDCID(faid).get(i)))).start();

        }
    }
}
