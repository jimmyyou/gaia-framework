package gaiaframework.gaiaagent;

import com.google.common.util.concurrent.RateLimiter;
import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.transmission.DataChunkMessage;
import gaiaframework.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This fetcher fetches all file blocks for a given FlowGroup. Version 2.0
 * It reads flowInfo, rate/path from FlowGroupInfo.
 * Design choice: use multi-producer multi-consumer model.
 */

public class FlowGroupFetcher {
    private static final Logger logger = LogManager.getLogger();

    FlowGroupInfo flowGroupInfo;
    LinkedBlockingQueue<DataChunkMessage> dataChunkInputQueue = new LinkedBlockingQueue<>();

    class FileFetcherThread implements Runnable {

        private final long startOffset;
        private final long totalBlockLength;
        private final String srcIP;
        private final String dstIP;
        private final String srcFilename;
        private final String dstFilename;

        // TODO(future) use fixed blocksize for now, may need to change in the future
        private final int chunkSize;
        private final String blockId;

        LinkedBlockingQueue<DataChunkMessage> dataQueue;
        ShuffleInfo.FlowInfo flowInfo;

        public FileFetcherThread(ShuffleInfo.FlowInfo flowInfo, LinkedBlockingQueue<DataChunkMessage> dataQueue, int chunkSize) {
            this.flowInfo = flowInfo;
            this.dataQueue = dataQueue;
            this.srcIP = flowInfo.getMapperIP();
            this.dstIP = flowInfo.getReducerIP();
            this.startOffset = flowInfo.getStartOffSet();
            this.totalBlockLength = flowInfo.getFlowSize();
            this.chunkSize = chunkSize;
            this.srcFilename = flowInfo.getDataFilename();
            this.blockId = flowInfo.getReduceAttemptID();

            this.dstFilename = Constants.getDstFileName(flowInfo); // TODO MUST FIX this.
        }

        @Override
        public void run() {

            // first get the URL
//        http://localhost:20020/home/jimmy/Downloads/profile.xml?start=0&len=5
            StringBuilder str_url = new StringBuilder("http://").append(srcIP).append(':').append(Constants.DEFAULT_HTTP_SERVER_PORT)
                    .append(srcFilename).append("?start=").append(startOffset).append("&len=").append(totalBlockLength);

            RateLimiter rateLimiter = RateLimiter.create(Constants.DEFAULT_TOKEN_RATE);
//        logger.info("Fetcher started with freq {}", Constants.DEFAULT_TOKEN_RATE);

            try {
                URL url = new URL(str_url.toString());
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");

                // Useful?
                connection.setRequestProperty("Connection", "Keep-Alive");
                connection.setRequestProperty("Keep-Alive", "header");

                connection.connect();

                // then set up the connection, and convert the stream into queue
                DataInputStream input = new DataInputStream(connection.getInputStream());

                // No need to get file length
//            long filelength = connection.getHeaderFieldLong("x-FileLength", 0);

                // Use best effort to fill in the queue.
                int total_bytes_sent = 0;
                while (true) {

                    // Define the max buffer
                    byte[] buf = new byte[chunkSize];
                    int data_length = chunkSize;

                    // Make sure we will not read EOF.
                    if (data_length + total_bytes_sent > totalBlockLength) {
                        data_length = (int) (totalBlockLength - total_bytes_sent);
                    }

                    try {
                        input.readFully(buf, 0, data_length);
                        DataChunkMessage dm = new DataChunkMessage(dstFilename, dstIP, blockId, (total_bytes_sent), totalBlockLength, 0, buf);
                        total_bytes_sent += data_length;

                        dataQueue.put(dm);

                        //                        agentSharedData.workerQueues.get(faID)[pathID].put(new CTRL_to_WorkerMsg(dm));
                    } catch (EOFException e) {
                        logger.error("ERROR: Reading EOF from {}, NOP", srcFilename);

                    }
                }

            } catch (MalformedURLException e) {
                e.printStackTrace();
                logger.error("URL malformed: {}", str_url.toString());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }


    public FlowGroupFetcher(FlowGroupInfo flowGroupInfo) {
        this.flowGroupInfo = flowGroupInfo;
    }


    /**
     * Start the Fetcher
     */
    public void start() {
        // TODO implement FlowGroupFetcher
        // HOWTO: implement a runnable to fetch through flowInfoList

    }
}
