package gaiaframework.transmission;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;


public class BestEffortForwardingThread implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    private final LinkedBlockingQueue<DataChunkMessage> inputQueue;

    public Socket dataSocket;
    private String raIP;
    private int raPort;

    private ObjectOutputStream oos;

    public BestEffortForwardingThread(LinkedBlockingQueue<DataChunkMessage> queue, String raIP, int raPort) {
        this.raIP = raIP;
        this.raPort = raPort;

        this.inputQueue = queue;
    }

    @Override
    public void run() {

        // first connect
        connectSoc_Retry();

        // then enter loop

        while (true) {

            try {

                DataChunkMessage data = inputQueue.take();

                oos.writeObject(data);

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    private void connectSoc_Retry() {

        boolean isConnected = false;
        while (!isConnected) {
            try {
                dataSocket = new Socket(raIP, raPort);
//                dataSocket.setSoTimeout(Constants.DEFAULT_SOCKET_TIMEOUT);
                dataSocket.setKeepAlive(true);

                logger.info("FA connected to {} : {} from port {}, keepAlive {}", raIP, raPort, dataSocket.getPort(), dataSocket.getKeepAlive());
            } catch (IOException e) {
                logger.error("Error while connecting to {} {} from port {}", raIP, raPort, dataSocket.getPort());
                e.printStackTrace();

                // sleep for some time
                try {
                    logger.error("Retry-connection in {} seconds", 5);
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                continue;
            }

            isConnected = true;
        }

        try {
            oos = new ObjectOutputStream(dataSocket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
