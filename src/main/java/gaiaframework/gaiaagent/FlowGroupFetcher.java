package gaiaframework.gaiaagent;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This fetcher fetches all file blocks for a given FlowGroup. Version 2.0
 * It reads flowInfo, rate/path from FlowGroupInfo.
 * Design choice: use multi-producer multi-consumer model.
 */

public class FlowGroupFetcher {
    private static final Logger logger = LogManager.getLogger();

    FlowGroupInfo flowGroupInfo;
    LinkedBlockingQueue<DataChunkMessage> dataChunkInputQueue = new LinkedBlockingQueue<>(Constants.FETCHER_QUEUE_LENGTH);

    /**
     * FileFetcher fetches one block of file, into the buffer.
     */
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

        public FileFetcherThread(ShuffleInfo.FlowInfo flowInfo, LinkedBlockingQueue<DataChunkMessage> dataQueue, int chunkSize, String fgID) {
            this.flowInfo = flowInfo;
            this.dataQueue = dataQueue;
            this.srcIP = flowInfo.getMapperIP();
            this.dstIP = flowInfo.getReducerIP();
            this.startOffset = flowInfo.getStartOffSet();
            this.totalBlockLength = flowInfo.getFlowSize();
            this.chunkSize = chunkSize;
            this.srcFilename = flowInfo.getDataFilename();
            this.blockId = flowInfo.getReduceAttemptID();

            this.dstFilename = Constants.getDstFileName(flowInfo, fgID);
        }

        @Override
        public void run() {
            logger.info("Starting FileFetcherThread for {}", dstFilename);

            StringBuilder str_url = new StringBuilder("http://").append(srcIP).append(':').append(Constants.DEFAULT_HTTP_SERVER_PORT)
                    .append(srcFilename).append("?start=").append(startOffset).append("&len=").append(totalBlockLength);

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

            logger.info("Finishing FileFetcherThread for {}", dstFilename);
        }
    }

    /**
     * RateEnforcer fetches from the buffer and enforce rate/path allocation
     * Each enforcer corresponds to a path, and enforce the path on that specific path.
     */
    class RateEnforcerThread implements Runnable {

        // TODO implement RateEnforcerThread
        // HOWTO enforce rate/path allocation?
        // Because now we use constant chunkSize, we can simply wait for a given time, then try to fetch from queue.
        @Override
        public void run() {

        }
    }

    public FlowGroupFetcher(FlowGroupInfo flowGroupInfo) {
        this.flowGroupInfo = flowGroupInfo;
    }


    /**
     * Start the Fetcher.
     * First start producer(fetcher thread for file blocks). Then start consumer(dispatcher/rate limiter)
     */
    public void start() {

        // Use a threadpool to exec through all flowinfo
        ExecutorService executor = Executors.newFixedThreadPool(1);

        for (ShuffleInfo.FlowInfo finfo : flowGroupInfo.flowInfos) {
            executor.submit(new FileFetcherThread(finfo, dataChunkInputQueue, Constants.HTTP_CHUNKSIZE, flowGroupInfo.fgID));
        }

        // TODO also need to start consumer (rate enforcers)


        // TODO need to stop producer and consumer after transmission. Also needs to record the status.
//        for()
//        Thread fft = new Thread(new FileFetcherThread());
    }
}
