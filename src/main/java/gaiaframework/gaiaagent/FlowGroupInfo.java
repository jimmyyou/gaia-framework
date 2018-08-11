package gaiaframework.gaiaagent;

/**
 * Stores information of FlowGroup for Sending Agent. version 2.0
 */

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.RateLimiter;
import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class FlowGroupInfo {

    private static final Logger logger = LogManager.getLogger();
    final String fgID;
    final String faID;
    final int pathSize;
    final long totalVolume;
    final AgentSharedData agentSharedData;

    // Because we create one chunk for each path, so we don't really need total rate. (we will sum up the rate on the fly)
    //    HashMap<Integer, RateLimiter> rateLimiterHashMap = new HashMap<>();
    final ArrayList<RateLimiter> rateLimiterArrayList;
    final ArrayList<AtomicDouble> pathTokenRateArrayList;

    @Deprecated
    LinkedList<ShuffleInfo.FlowInfo> flowInfos = new LinkedList<>();

    volatile long remainingVolume;
//    volatile double rate;

    volatile boolean finished = false;
    AtomicBoolean isFinished = new AtomicBoolean(false);

//    Thread fetcher;

    public FlowGroupInfo(AgentSharedData agentSharedData, String forwardingAgentID, String fgID, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fue) {

        this.fgID = fgID;
        this.faID = forwardingAgentID;
        this.agentSharedData = agentSharedData;
        this.pathSize = this.agentSharedData.netGraph.apap_.get(this.agentSharedData.saID).get(faID).size();
        this.rateLimiterArrayList = new ArrayList<>(pathSize);
        this.pathTokenRateArrayList = new ArrayList<>(pathSize);

        if (fue.getOp() != GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Operation.START) {
            logger.error("FATAL: starting FG {}, but OP != START", fgID);
        }

        logger.info("FlowGroupInfo: summing volume");
        this.totalVolume = fue.getFlowInfosList().stream().mapToLong(ShuffleInfo.FlowInfo::getFlowSize).sum();
        this.remainingVolume = totalVolume;
//        logger.info("");
//        this.remainingVolume = fue.getRemainingVolume();
//        this.totalVolume = fue.getRemainingVolume();
        logger.info("FlowGroupInfo: add flowInfos");
        this.flowInfos.addAll(fue.getFlowInfosList());

        // init rateLimiterArrayList

        for (int i = 0; i < pathSize; i++) {
            RateLimiter rateLimiter = RateLimiter.create(Constants.DEFAULT_TOKEN_RATE);
            this.rateLimiterArrayList.add(rateLimiter);
        }
        // init pathTokenRateArrayList
        setRateLimiters(fue.getPathIDToRateMapMap());
    }

    /**
     * Method to set the rates for this FlowGroup, must be synchronized.
     *
     * @param PathToRateMap
     */
    public synchronized void setRateLimiters(Map<Integer, Double> PathToRateMap) {

        // Iterate through all rate limiters. set the rate.
        for (int i = 0; i < pathSize; i++) {
            pathTokenRateArrayList.add(new AtomicDouble(0));

            // Search for rate in Entries
            if (PathToRateMap.containsKey(i)) {
                logger.info("setRateLimiters() set path {} to rate {}", i, PathToRateMap.get(i));
                double tokenRate = PathToRateMap.get(i) / Constants.HTTP_CHUNKSIZE;
                if (tokenRate > Constants.DOUBLE_EPSILON) {
                    // TODO verify this: convert to token rate. also check if rate is zero
                    logger.info("{} : {} token rate to {}", fgID, i, tokenRate);
                    rateLimiterArrayList.get(i).setRate(tokenRate);
                    pathTokenRateArrayList.get(i).set(tokenRate);
                } else {
                    // rate can't be zero
                    rateLimiterArrayList.get(i).setRate(Constants.DEFAULT_TOKEN_RATE);
                    pathTokenRateArrayList.get(i).set(0);
                }
            } else {
                // This path has zero rate.
                // rate can't be zero
                rateLimiterArrayList.get(i).setRate(Constants.DEFAULT_TOKEN_RATE);
                pathTokenRateArrayList.get(i).set(0);
            }
        }
    }

    public void setPauseFlowGroup() {
        // pause Flow Group by setting rate limiter to 0
        // Iterate through all rate limiters. set the rate.
        for (int i = 0; i < pathSize; i++) {
            // rate can't be zero
            rateLimiterArrayList.get(i).setRate(Constants.DEFAULT_TOKEN_RATE);
            pathTokenRateArrayList.get(i).set(0);
        }
    }

    public synchronized void onTransmit(int length) {
        remainingVolume -= length;
        if (remainingVolume == 0) {
            if (!isFinished.getAndSet(true)) {
                // We are the first to observe the finish
                agentSharedData.finishFlow(fgID);
            }
        }
    }
}
