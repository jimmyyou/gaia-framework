package gaiaframework.gaiaagent;

// Best Effort worker Thread

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

public class SimpleBestEfforWorker implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    //    PConnection conn;
    AgentSharedData sharedData;

    String connID; // name of this TCP Connection. SA_id-RA_id.path_id
    String raID;
    int pathID;

    // Queue on which SendingAgent places updates for this PersistentConnection. Updates
    // flow, or that the PersistentConnection should terminate.
    // may inform the PersistentConnection of a new subscribing flow, an unsubscribing
    LinkedBlockingQueue<CTRL_to_WorkerMsg> inputQueue;


    public Socket dataSocket;
    private String faIP;
    private int faPort;
    private int localPort;

    //    private BufferedOutputStream bos;
    private ObjectOutputStream oos;

    public SimpleBestEfforWorker(String conn_id, String RAID, int pathID, LinkedBlockingQueue inputQueue,
                                 AgentSharedData sharedData, String faip, int faPort, int port) {
        this.connID = conn_id;
        this.inputQueue = inputQueue;
        this.sharedData = sharedData;
        this.raID = RAID;
        this.pathID = pathID;
        this.faIP = faip;
        this.faPort = faPort;
        this.localPort = port;

    }

    @Override
    public void run() {
        CTRL_to_WorkerMsg m = null;

        // 1 await for the READY signal before entering the main eventloop
        try {
            sharedData.readySignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        // 2 wait for the CONNECT msg from the CTRL
        while (true) {
            try {
                m = inputQueue.take();

                if (m.type == CTRL_to_WorkerMsg.MsgType.CONNECT) {
                    connectSoc_Retry();
                    break;
                } else {
                    logger.error("Expecting CONNECT msg, got {}", m.type);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // 3 enter eventloop
        while (true) {
            try {
                m = inputQueue.take();

                if (m.getType() == CTRL_to_WorkerMsg.MsgType.DATA) {

                    // send data
                    oos.writeObject(m.dataChunkMessage);
//                    logger.info("Worker written data {} to {}", m.dataChunkMessage.getStartIndex(), dataSocket.getLocalSocketAddress());


                }

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
                dataSocket = new Socket(faIP, faPort, null, localPort);
//                dataSocket.setSoTimeout(Constants.DEFAULT_SOCKET_TIMEOUT);
                dataSocket.setKeepAlive(true);

                logger.info("Worker {} connected to {} : {} from {}, keepAlive {}", connID, faIP, faPort, dataSocket.getLocalSocketAddress(), dataSocket.getKeepAlive());
            } catch (IOException e) {
                logger.error("Error while connecting to {} {} from port {}", faIP, faPort, localPort);
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

        sharedData.cnt_StartedConnections.countDown();
        sharedData.activeConnections.incrementAndGet();

/*        try {
            dataSocket = new Socket(faIP, faPort, null, localPort);
            dataSocket.setKeepAlive(true);

            logger.info("Worker {} connected to {} : {} from port {}, keepAlive: {}", connID, faIP, faPort, localPort, dataSocket.getKeepAlive());

            oos = new ObjectOutputStream(dataSocket.getOutputStream());

            sharedData.cnt_StartedConnections.countDown();
            sharedData.activeConnections.incrementAndGet();

        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }

}
