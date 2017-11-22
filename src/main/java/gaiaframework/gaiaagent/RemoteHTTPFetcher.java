package gaiaframework.gaiaagent;

// Similar to LocalFileReader, but use HTTP to fetch data instead
// fetches from ShuffleHandler of Hadoop

import com.google.common.util.concurrent.RateLimiter;
import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.transmission.DataChunkMessage;
import gaiaframework.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class RemoteHTTPFetcher implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    String srcFilename;
    String dstFilename;
    // FIXME use real IP addr!!!
    String dstIP;
    String srcIP = "localhost";

    AgentSharedData agentSharedData;

    // no need for dataQueue!!!, directly put into worker Queue
//    LinkedBlockingQueue<DataChunkMessage> dataQueue;

    long startOffset;
    long totalLength;

    long readBytes = 0;

    HttpURLConnection connection;

    FlowGroupInfo owningFlowGroupInfo;

//    public RemoteHTTPFetcher(String srcFilename, LinkedBlockingQueue<DataChunk> dataQueue, long totalFileSize) {
//        this.srcFilename = srcFilename;
//        this.dataQueue = dataQueue;
//        this.totalSize = totalFileSize;
//    }

    public RemoteHTTPFetcher(FlowGroupInfo flowGroupInfo, ShuffleInfo.FlowInfo flowInfo, LinkedBlockingQueue<DataChunkMessage> dataQueue, String srcHostIP, String dstHostIP) {
        this.startOffset = flowInfo.getStartOffSet();
        this.totalLength = flowInfo.getFlowSize();

        this.srcFilename = flowInfo.getDataFilename();
        this.dstFilename = getDstFilename(srcFilename, flowInfo.getReduceAttemptID());
//        this.dataQueue = dataQueue;

        this.srcIP = srcHostIP;
        this.dstIP = dstHostIP;

        this.owningFlowGroupInfo = flowGroupInfo;
        this.agentSharedData = owningFlowGroupInfo.agentSharedData;


    }

    // TODO verify
    private String getDstFilename(String srcFilename, String reduceAttemptID) {
        String ret = srcFilename.substring(0, srcFilename.length()-4) + "_" + reduceAttemptID + ".out";
        return ret;
    }


    @Override
    public void run() {
        // first get the URL

        URL url = getURL();

        RateLimiter rateLimiter = RateLimiter.create(Constants.DEFAULT_TOKEN_RATE);

        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            // Useful?
            connection.setRequestProperty("Connection", "Keep-Alive");
            connection.setRequestProperty("Keep-Alive", "header");

            connection.connect();

            // then set up the connection, and convert the stream into queue
            DataInputStream input = new DataInputStream(connection.getInputStream());

            int total_bytes_sent = 0;
            while (true) {

                int cur_bytes_sent = 0;

                // Each time before start fetching, first learn about current rate
                List<AggFlowGroupInfo.WorkerInfo> wiList = new LinkedList<>();
                double totalRate = learnRate(wiList);

                double cur_rate = totalRate;

                int data_length;

//            FetchData according to the totalRate
                // check if 100 permits/s is enough (3200MByte/s enough?)
                if (cur_rate < Constants.BLOCK_SIZE_MB * 8 * Constants.DEFAULT_TOKEN_RATE) {
                    // no need to change rate , calculate the length
                    rateLimiter.setRate(Constants.DEFAULT_TOKEN_RATE);
                    data_length = (int) (cur_rate / Constants.DEFAULT_TOKEN_RATE * 1024 * 1024 / 8);
                } else {
                    data_length = Constants.BLOCK_SIZE_MB;
                    double new_rate = cur_rate / 8 / Constants.BLOCK_SIZE_MB;
                    logger.error("Total rate {} too high for {}, setting new sending rate to {} / s", totalRate, srcFilename, new_rate);
                    rateLimiter.setRate(new_rate); // TODO: verify that the rate is enforced , since here we (re)set the rate for each chunk
                }

                // rate limiting
                rateLimiter.acquire(1);

                // see if we can read any more bytes?

                if (data_length + readBytes > totalLength) {
                    data_length = (int) (totalLength - readBytes);
                }

                byte[] buf = new byte[data_length];
                int bytes_read = input.read(buf, 0, data_length);

                if (bytes_read == -1){
                    logger.info("Finished reading from {}", srcFilename);
                    break;
                }

                if (bytes_read != data_length) {
                    logger.error("bytes read is smaller than expected");
                }

                // send data through all paths
                for (int i = 0; i < wiList.size(); i++) {

                    String faID = wiList.get(i).getRaID();
                    int pathID = wiList.get(i).getPathID();

                    int thisChunkSize = 0;
                    if( i < wiList.size() - 1) {
                        thisChunkSize = (int) (wiList.get(i).rate / totalRate * bytes_read);
                    }
                    else { // the last piece of this block
                        thisChunkSize = data_length - cur_bytes_sent;
                    }

                    // copy the chunkbuf
                    byte[] chunkBuf = new byte[thisChunkSize];
                    for (int j = 0 ; j < thisChunkSize; j ++) {
                        chunkBuf[j] = buf[j + cur_bytes_sent];
                    }

                    if (total_bytes_sent == 0) { // first chunk

                        DataChunkMessage dm = new DataChunkMessage(dstFilename, dstIP, -1, totalLength, chunkBuf );
                        agentSharedData.workerQueues.get(faID)[pathID].put(new CTRL_to_WorkerMsg(dm));

                    }
                    else {

                        DataChunkMessage dm = new DataChunkMessage(dstFilename, dstIP, total_bytes_sent, thisChunkSize, chunkBuf );
                        agentSharedData.workerQueues.get(faID)[pathID].put(new CTRL_to_WorkerMsg(dm));

                    }

                    total_bytes_sent += thisChunkSize;
                    cur_bytes_sent += thisChunkSize;

                    logger.info("Sent {} for {}", total_bytes_sent, dstFilename);

                }

            }


        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Connecting to Shuffle Server failed.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    private double learnRate(List<AggFlowGroupInfo.WorkerInfo> wiList) {
        double totalRate = 0;

//        HashMap<AggFlowGroupInfo.WorkerInfo, Double> workerToRateMap = new HashMap<>();

        for (Map.Entry<String, ArrayList<ConcurrentHashMap<String, SubscriptionInfo>>> e1 : agentSharedData.subscriptionRateMaps.entrySet()) {

            String raID = e1.getKey();
            ArrayList<ConcurrentHashMap<String, SubscriptionInfo>> listByPath = e1.getValue();

            for (int pathID = 0; pathID < listByPath.size(); pathID++) {
                ConcurrentHashMap<String, SubscriptionInfo> rateMap = listByPath.get(pathID);

                // now we've determined a worker

                if (rateMap.size() > 1) {
                    logger.error("rateMap should not be > 1!");
                }

                if (rateMap.containsKey(owningFlowGroupInfo.parentFlowInfo.ID)) {
                    // we need to consider this rate!

                    double rate = rateMap.get(owningFlowGroupInfo.parentFlowInfo.ID).getRate();
                    totalRate += rate;

                    AggFlowGroupInfo.WorkerInfo wi = new AggFlowGroupInfo.WorkerInfo(raID, pathID, rate);

                    wiList.add(wi);
                }

            }
        }

        return totalRate;
    }

    private URL getURL() {
//        http://localhost:20020/home/jimmy/Downloads/profile.xml?start=0&len=5
        StringBuilder str_url = new StringBuilder("http://").append(srcIP).append(':').append(Constants.DEFAULT_HTTP_SERVER_PORT)
                .append(srcFilename).append("?start=").append(startOffset).append("&len=").append(totalLength);

        try {
            URL url = new URL(str_url.toString());
            return url;
        } catch (MalformedURLException e) {
            e.printStackTrace();
            logger.error("URL malformed");
//            return null;
        }

        return null;
    }


}
