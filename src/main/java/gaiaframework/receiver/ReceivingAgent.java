package gaiaframework.receiver;

// Receiving Agent, only used to receive data

import gaiaframework.gaiaagent.DataChunk;
import gaiaframework.util.Constants;
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

    public static void main(String[] args) {

        int port = 33330;

        if (args.length == 1) {
            port = Integer.parseInt(args[0]);
            logger.info("Running with 1 argument, set port to {}", port);
        }

        startNormalServer(port);

    }


    private static void startNormalServer(int port) {

        ServerSocket sd;
        int conn_cnt = 0;

        try {
            sd = new ServerSocket(port);
            sd.setReceiveBufferSize(64*1024*1024);
            System.err.println("DEBUG, serversocket buffer: " + sd.getReceiveBufferSize());
//            sd.setSoTimeout(0);

            Runtime.getRuntime().addShutdownHook(new Thread(){public void run(){
                try {
                    sd.close();
                    System.out.println("The server is shut down!");
                } catch (IOException e) { System.exit(1); }
            }});

            // start filewriter thread
            fileWriterThread = new Thread(new FileWriter(dataQueue));
            fileWriterThread.start();

            while (true) {
                Socket dataSocket = sd.accept();
//                dataSocket.setSoTimeout(Constants.DEFAULT_SOCKET_TIMEOUT);
                conn_cnt ++;
//                dataSocket.setSendBufferSize(16*1024*1024);
                dataSocket.setReceiveBufferSize(64*1024*1024);
                System.err.println("dataSoc buf " + dataSocket.getReceiveBufferSize());
//                dataSocket.setSoTimeout(0);
//                dataSocket.setKeepAlive(true);
                logger.info( "{} Got a connection from {}", conn_cnt , dataSocket.getRemoteSocketAddress().toString());
                (new Thread(new Receiver(dataSocket, dataQueue))).start();
            }
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
//            System.exit(1);
        }
    }

}
