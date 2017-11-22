package gaiaframework.transmission;

import gaiaframework.util.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

// simply reads the ChunkHeader and forwards it

public class ForwardingServer implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    private final Socket socket;
    private final ObjectInputStream ois;
    //    private final Configuration configuration;
    private final List<String> hostIPList;
    private final List<LinkedBlockingQueue<DataChunkMessage>> fwQueues;

    public ForwardingServer(List<String> hostIPList, Socket dataSocket, List<LinkedBlockingQueue<DataChunkMessage>> forwardingQueues) throws IOException {
        this.socket = dataSocket;

        this.ois = new ObjectInputStream(dataSocket.getInputStream());

        this.hostIPList = hostIPList;
        this.fwQueues = forwardingQueues;

    }

    @Override
    public void run() {


        while (true) {
            try {

                DataChunkMessage dataChunk = (DataChunkMessage) ois.readObject();

//                logger.info("Processing data {} {} {}\n{} {}", dataChunk.getFilename(), dataChunk.getStartIndex(),
//                        dataChunk.getChunkLength(), (int) dataChunk.getData()[0], (int) dataChunk.getData()[1]);

                if (dataChunk != null) {

                    String dstIP = dataChunk.getDestURL();
                    // No need to remove the ":"
                    int hostID = findHostIDbyIP(dstIP);

                    if (hostID >= 0 && hostID < hostIPList.size()) {

                        fwQueues.get(hostID).put(dataChunk);

                    } else {
                        logger.error("Found invalid hostID {} for IP {}", hostID, dstIP);
                    }


                } else {
                    logger.error("dataChunk == null");
                }
            } catch (EOFException e) {
                logger.error("EOF Exception caught, exiting...");
                break;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private int findHostIDbyIP(String dstIP) {
        for (int i = 0; i < hostIPList.size(); i++) {
            if (hostIPList.get(i).equals(dstIP)) {
                return i;
            }
        }

        return -1;
    }
}
