package gaiaframework.receiver;

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
    ReceivingAgentSharedData rasd;

    public Receiver(Socket client_sd, LinkedBlockingQueue<DataChunkMessage> dataQueue, ReceivingAgentSharedData rasd) throws java.io.IOException {
        sd_ = client_sd;
        in_ = new ObjectInputStream(client_sd.getInputStream());
        this.dataQueue = dataQueue;
        this.rasd = rasd;
    }

    public void run() {
//        byte[] buffer = new byte[1024*1024];
//        int num_recv;

        while (true) {
            try {

                DataChunkMessage dataChunk = (DataChunkMessage) in_.readObject();

//                logger.info("Processing data {} {} {}\n{} {}", dataChunk.getFilename(), dataChunk.getChunkStartIndex(),
//                        dataChunk.getTotalBlockLength(), (int) dataChunk.getData()[0], (int) dataChunk.getData()[1]);

                if (dataChunk != null) {

                    // dataQueue.put(dataChunk);
                    // don't put in queue, process the chunk immediately by forking a thread
                    long startTime = System.currentTimeMillis();

                    rasd.processData(dataChunk);
                    // TODO we may need to synchronize on file handler though, so for each file, the incoming blocks are queued on fileHandler

                    long deltaTime = System.currentTimeMillis() - startTime;
                    logger.info("ProcessData took {} ms", deltaTime);


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

