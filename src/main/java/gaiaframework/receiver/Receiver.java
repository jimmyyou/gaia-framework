package gaiaframework.receiver;

import gaiaframework.gaiaagent.DataChunk;
import gaiaframework.transmission.DataChunkMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;


public class Receiver implements Runnable {
    private static final Logger logger = LogManager.getLogger();
    public Socket sd_;
    public ObjectInputStream in_;

    LinkedBlockingQueue<DataChunkMessage> dataQueue;

    public Receiver(Socket client_sd, LinkedBlockingQueue<DataChunkMessage> dataQueue) throws java.io.IOException {
        sd_ = client_sd;
        in_ = new ObjectInputStream(client_sd.getInputStream());
        this.dataQueue = dataQueue;
    }

    public void run() {
//        byte[] buffer = new byte[1024*1024];
//        int num_recv;

        while (true) {
            try {

                DataChunkMessage dataChunk = (DataChunkMessage) in_.readObject();

//                logger.info("Processing data {} {} {}\n{} {}", dataChunk.getFilename(), dataChunk.getStartIndex(),
//                        dataChunk.getChunkLength(), (int) dataChunk.getData()[0], (int) dataChunk.getData()[1]);

                if (dataChunk != null) {
                    dataQueue.put(dataChunk);
                } else {
                    logger.error("dataChunk == null");
                }

////                num_recv = in_.read(buffer);
//                if (num_recv < 0) {
//                    logger.info("SocketInputStream.read() returns {}" , num_recv);
//                    break;
//                }
            } catch (java.io.IOException e) {
                logger.error("IOException caught");
                e.printStackTrace();
                break;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        logger.info("Closing socket from {}", sd_.getRemoteSocketAddress());

        try {
            in_.close();
            sd_.close();
        } catch (java.io.IOException e) {
            logger.error("Error closing socket");
            e.printStackTrace();
//            System.exit(1);
        }
    }

}

