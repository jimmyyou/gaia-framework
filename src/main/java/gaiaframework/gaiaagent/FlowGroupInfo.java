package gaiaframework.gaiaagent;

/**
 * Stores information of FlowGroup for Sending Agent. version 2.0
 */

import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.gaiaprotos.GaiaMessageProtos;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class FlowGroupInfo {

    private static final Logger logger = LogManager.getLogger();
    final String fgID;
    final String faID;

    // Because we create one chunk for each path, so we don't really need total rate. (we will sum up the rate on the fly)
    List<GaiaMessageProtos.FlowUpdate.PathRateEntry> pathRateEntries;

    LinkedList<ShuffleInfo.FlowInfo> flowInfos = new LinkedList<>();

    volatile double remainingVolume;
//    volatile double rate;

    AgentSharedData agentSharedData;

//    Thread fetcher;

    public FlowGroupInfo(String faID, String fgID, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fue) {

        this.fgID = fgID;
        this.faID = faID;

        if (fue.getOp() != GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Operation.START) {
            logger.error("FATAL: starting FG {}, but OP != START", fgID);
        }

        this.remainingVolume = fue.getRemainingVolume();
        this.flowInfos.addAll(fue.getFlowInfosList());

        // store pathToRate Info
        setPathRateEntries(fue.getPathToRateList());

    }

    /**
     * Method to read the rates for this FlowGroup, must be synchronized.
     *
     * @return
     */
    public synchronized List<GaiaMessageProtos.FlowUpdate.PathRateEntry> getPathRateEntries() {
        return pathRateEntries;
    }

    /**
     * Method to set the rates for this FlowGroup, must be synchronized.
     *
     * @param pathRateEntries
     */
    public synchronized void setPathRateEntries(List<GaiaMessageProtos.FlowUpdate.PathRateEntry> pathRateEntries) {
        this.pathRateEntries = pathRateEntries;
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
}
