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

public class FlowGroupInfo {

    private static final Logger logger = LogManager.getLogger();
    final String fgID;
    final String faID;
    final int pathSize;

    // Because we create one chunk for each path, so we don't really need total rate. (we will sum up the rate on the fly)
    //    HashMap<Integer, RateLimiter> rateLimiterHashMap = new HashMap<>();
    ArrayList<RateLimiter> rateLimiterArrayList;

    LinkedList<ShuffleInfo.FlowInfo> flowInfos = new LinkedList<>();

    volatile double remainingVolume;
//    volatile double rate;

    AgentSharedData agentSharedData;

//    Thread fetcher;

    public FlowGroupInfo(String forwardingAgentID, String fgID, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fue) {

        this.fgID = fgID;
        this.faID = forwardingAgentID;
        this.pathSize = agentSharedData.netGraph.apap_.get(agentSharedData.saID).get(faID).size();
        this.rateLimiterArrayList = new ArrayList<>(pathSize);

        if (fue.getOp() != GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Operation.START) {
            logger.error("FATAL: starting FG {}, but OP != START", fgID);
        }

        this.remainingVolume = fue.getRemainingVolume();
        this.flowInfos.addAll(fue.getFlowInfosList());

        // store pathToRate Info
        setPathRateEntries(fue.getPathIDToRateMapMap());

    }

    /**
     * Method to set the rates for this FlowGroup, must be synchronized.
     *
     * @param PathToRateMap
     */
    public synchronized void setPathRateEntries(Map<Integer, Double> PathToRateMap) {

        // Iterate through all rate limiters. set the rate.
        for (int i = 0; i < pathSize; i++) {
            // Search for rate in Entries
            if (PathToRateMap.containsKey(i)) {
                // TODO verify this: convert to token rate.
                rateLimiterArrayList.get(i).setRate(PathToRateMap.get(i) / Constants.HTTP_CHUNKSIZE);
            } else {
                // This path has zero rate.
                rateLimiterArrayList.get(i).setRate(0);
            }
        }
    }

    /**
     * Called by fetcher, to update stats after transmission.
     *
     * @param volume
     */
    public void transmit(long volume) {

        ArrayList<AggFlowGroupInfo> to_remove = new ArrayList<>();

        // TODO, verify FlowGroupInfo.transmit() after removing AggFlowGroupInfo.java
/*
        boolean done = parentFlowInfo.transmit(volume);
        if (done) { // meaning parent AggFlowGroup is done.
            logger.info("{} finished transmission", this.parentFlowInfo.getID());
            //
            // wait until GAIA told us to stop, then stop. (although might cause a problem here.)
            to_remove.add(parentFlowInfo);
        }
*/

        for (AggFlowGroupInfo afgi : to_remove) {

            String afgID = afgi.getID(); // fgID == fgiID

            // don't remove for now!
//            agentSharedData.aggFlowGroups.remove(afgi.getID());

            // maybe not needing to delete here?
//            agentSharedData.subscriptionRateMaps.get(faID).get(pathID).remove(afgID);

            agentSharedData.finishFlow(afgID);
        }
    }

    public void setPauseFlowGroup() {
        // TODO pause Flow Group here, by setting rate limiter to 0


    }
}
