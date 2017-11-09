package gaiaframework.receiver;

// Receiving Agent, only used to receive data

import gaiaframework.gaiaagent.DataChunk;
import gaiaframework.util.Configuration;
import gaiaframework.util.Constants;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

public class ReceivingAgent {

    private static final Logger logger = LogManager.getLogger();

    static LinkedBlockingQueue<DataChunk> dataQueue = new LinkedBlockingQueue<>();

    static Thread fileWriterThread;
    static boolean isOutputEnabled = true;

    public static void main(String[] args) {

        int port = 33330;

        Options options = new Options();
        options.addOption("p", "port", true, "port to listen on");
        options.addOption("n", "no-output-file", false, "do not write to file");

        HelpFormatter formatter = new HelpFormatter();

        if (args.length == 0) {
            formatter.printHelp("SendingAgent -p [port] -n", options);
            return;
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

        } catch (ParseException e) {
            e.printStackTrace();
        }

        startNormalServer(port, isOutputEnabled);

    }


    private static void startNormalServer(int port, boolean isOutputEnabled) {

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
            fileWriterThread = new Thread(new FileWriter(dataQueue, isOutputEnabled));
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
