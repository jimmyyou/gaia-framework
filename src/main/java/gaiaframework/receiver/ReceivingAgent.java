package gaiaframework.receiver;

// Receiving Agent, only used to receive data

import gaiaframework.transmission.DataChunkMessage;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;

public class ReceivingAgent {

    private static final Logger logger = LogManager.getLogger();

    static LinkedBlockingQueue<DataChunkMessage> dataQueue = new LinkedBlockingQueue<>();

    static Thread fileWriterThread;
    static boolean isOutputEnabled = true;

    public static void main(String[] args) {

        int port = 33330;
        String masterHostname = "localhost";

        Options options = new Options();
        options.addOption("p", "port", true, "port to listen on");
        options.addOption("n", "no-output-file", false, "do not write to file");
        options.addOption("mh", "master-hostname", true, "hostname of master of this DC.");

        HelpFormatter formatter = new HelpFormatter();

        if (args.length == 0) {
            formatter.printHelp("SendingAgent -p [port] -n", options);
//            return;
        }

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("n")) {
                logger.info("Disabling File output");
                isOutputEnabled = false;
            }

            if (cmd.hasOption("p")) {
                port = Integer.parseInt(cmd.getOptionValue('p'));
                logger.info("Using port {}", port);
            }

            if (cmd.hasOption("mh")) {
                masterHostname = cmd.getOptionValue("mh");
                logger.info("Using master hostname: {}", masterHostname);
            } else {
                // try to infer from local
                try {
                    InetAddress myHost = InetAddress.getLocalHost();
                    String localHostname = myHost.getHostName();
                    masterHostname = "dc" + localHostname.substring(1, 2) + "master";
                    logger.info("Inferring master hostname from {} : {}", localHostname, masterHostname);
                } catch (UnknownHostException ex) {
                    ex.printStackTrace();
                }
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

        startNormalServer(port, isOutputEnabled, masterHostname);

    }


    private static void startNormalServer(int port, boolean isOutputEnabled, String masterHostname) {

        ServerSocket sd;
        int conn_cnt = 0;

        try {
            sd = new ServerSocket(port);
            sd.setReceiveBufferSize(64 * 1024 * 1024);
            System.err.println("DEBUG, serversocket buffer: " + sd.getReceiveBufferSize());
//            sd.setSoTimeout(0);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    try {
                        sd.close();
                        System.out.println("The server is shut down!");
                    } catch (IOException e) {
                        System.exit(1);
                    }
                }
            });

            // start filewriter thread
            fileWriterThread = new Thread(new FileWriter(dataQueue, isOutputEnabled, masterHostname));
            fileWriterThread.start();

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
                (new Thread(new Receiver(dataSocket, dataQueue))).start();
            }
        } catch (java.io.IOException e) {
            e.printStackTrace();
//            System.exit(1);
        }
    }

}
