package gaiaframework.gaiaagent;

/**
 * Stores information of FlowGroup for Sending Agent. version 2.0
 */

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

    // Because we create one chunk for each path, so we don't really need total rate. (we will sum up the rate on the fly)
    //    HashMap<Integer, RateLimiter> rateLimiterHashMap = new HashMap<>();
    ArrayList<RateLimiter> rateLimiterArrayList;

    LinkedList<ShuffleInfo.FlowInfo> flowInfos = new LinkedList<>();

    volatile long remainingVolume;
//    volatile double rate;

    AgentSharedData agentSharedData;
    volatile boolean finished = false;
    AtomicBoolean isFinished = new AtomicBoolean(false);

//    Thread fetcher;

    public FlowGroupInfo(String forwardingAgentID, String fgID, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fue) {

        this.fgID = fgID;
        this.faID = forwardingAgentID;
        // TODO fix here! nullpointer!!
        if (agentSharedData.netGraph == null) {
            logger.info("DEBUG: netgrapch=null");
        } else if (agentSharedData.netGraph.apap_ == null){
            logger.info("apap=null");
        } else if (agentSharedData.netGraph.apap_.get(agentSharedData.saID) == null){
            logger.info("DEBUG: get(_)=null");
        } else if (agentSharedData.netGraph.apap_.get(agentSharedData.saID).get(faID) == null){
            logger.info("DEBUG get().get() = null");
        }
        this.pathSize = agentSharedData.netGraph.apap_.get(agentSharedData.saID).get(faID).size();
        this.rateLimiterArrayList = new ArrayList<>(pathSize);

        if (fue.getOp() != GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Operation.START) {
            logger.error("FATAL: starting FG {}, but OP != START", fgID);
        }

        this.totalVolume = fue.getFlowInfosList().stream().mapToLong(ShuffleInfo.FlowInfo::getFlowSize).sum();
        this.remainingVolume = totalVolume;
//        logger.info("");
//        this.remainingVolume = fue.getRemainingVolume();
//        this.totalVolume = fue.getRemainingVolume();
        this.flowInfos.addAll(fue.getFlowInfosList());

        // store pathToRate Info
        for (int i = 0; i < pathSize; i++) {
            RateLimiter rateLimiter = RateLimiter.create(Constants.DEFAULT_TOKEN_RATE);
            this.rateLimiterArrayList.add(rateLimiter);
        }
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
            // Search for rate in Entries
            if (PathToRateMap.containsKey(i)) {
                logger.info("setRateLimiters() set path {} to rate {}", i, PathToRateMap.get(i));
                // TODO verify this: convert to token rate.
                rateLimiterArrayList.get(i).setRate(PathToRateMap.get(i) / Constants.HTTP_CHUNKSIZE);
            } else {
                // This path has zero rate.
                rateLimiterArrayList.get(i).setRate(0);
            }
        }
    }

    public void setPauseFlowGroup() {
        // pause Flow Group by setting rate limiter to 0
        // Iterate through all rate limiters. set the rate.
        for (int i = 0; i < pathSize; i++) {
            rateLimiterArrayList.get(i).setRate(0);
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
