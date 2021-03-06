package gaiaframework.gaiamaster;

/**
 * FlowGroup v2.0 includes all flows from one site to another site in a coflow.
 * FlowGroup will directly be used in scheduler (scheduler does not see individual flows).
 * TerraMaster does not learn about progress of individual flows, only about progress of FlowGroups
 */

// TODO(future) clear definition of FG_FIN, and corresponding API (e.g. fg.isFinished())

import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.util.Constants;

import java.util.LinkedList;
import java.util.List;

public class FlowGroup {

    public List<ShuffleInfo.FlowInfo> flowInfos = new LinkedList<>();

    // final fields
    private final String id;
    private final String srcLocation;
    private final String dstLocation;
    private final String owningCoflowID;
    private final double totalVolume;

    // non-final fields
    private long startTime = -1;
    private long endTime = -1;

    private volatile double transmitted;

    private boolean isSendingFinished = false; // set true along with setting endTime.

    // the state of this flow
    public enum FlowGroupState {
        NEW,
        RUNNING,
        PAUSED,
        TRANSFER_FIN,
        FILE_FIN
    }

    private FlowGroupState flowGroupState = FlowGroupState.NEW;

/*    @Deprecated
    String mapID;
    @Deprecated
    String redID;*/

/*    @Deprecated
    public FlowGroup(String id, String srcLocation, String dstLocation, String owningCoflowID, double totalVolume, String filename, String mapID, String redID) {
        this.id = id;
        this.srcLocation = srcLocation;
        this.dstLocation = dstLocation;
        this.owningCoflowID = owningCoflowID;
        this.totalVolume = totalVolume;
        this.flowGroupState = FlowGroupState.NEW;
        this.mapID = mapID;
        this.redID = redID;
    }*/

    public FlowGroup(String cfID, String srcLoc, String dstLoc, List<ShuffleInfo.FlowInfo> flowInfos) {
        this.id = cfID + ":" + srcLoc + "-" + dstLoc;
        this.srcLocation = srcLoc;
        this.dstLocation = dstLoc;
        this.owningCoflowID = cfID;
        this.flowInfos = flowInfos;
        this.totalVolume = flowInfos.stream().mapToLong(ShuffleInfo.FlowInfo::getFlowSize).sum();
        // TODO(future) check the long to double conversion here
    }

    // This method is called upon receiving Status Update, this method must be call if a Flow is finishing
    // if a flow is already marked isSendingFinished, we don't invoke coflowFIN
    public synchronized boolean getAndSetFinish(long timestamp) {
        if (isSendingFinished && this.transmitted + Constants.DOUBLE_EPSILON >= totalVolume) { // if already isSendingFinished, do nothing
            return true;
        } else { // if we are the first thread to finish it
            this.transmitted = this.totalVolume;
            this.endTime = timestamp;
            this.isSendingFinished = true;
            this.flowGroupState = FlowGroupState.TRANSFER_FIN;
            return false;
        }
    }

    public synchronized boolean getAndSetFileFIN() {
        if (this.flowGroupState == FlowGroupState.TRANSFER_FIN) {
            // If we are the first thread to receive FILE_FIN
            this.flowGroupState = FlowGroupState.FILE_FIN;
            return false;

        } else if (this.flowGroupState == FlowGroupState.FILE_FIN) {
            return true;
        }

        return false;
    }

    public synchronized void setStartTime(long timestamp) {
        this.startTime = timestamp;
    }

    public synchronized void setTransmitted(double txed) {
        this.transmitted = txed;
    }

    public String getId() {
        return id;
    }

    public double getTotalVolume() {
        return totalVolume;
    }

    public double getRemainingVolume() {
        return totalVolume - transmitted;
    }

    public String getSrcLocation() {
        return srcLocation;
    }

    public String getDstLocation() {
        return dstLocation;
    }

    public String getOwningCoflowID() {
        return owningCoflowID;
    }

    public double getTransmitted() {
        return transmitted;
    }

/*    public static FlowGroup_Old_Compressed toFlowGroup_Old(FlowGroup fg, int intID) {
        FlowGroup_Old_Compressed fgo = new FlowGroup_Old_Compressed(fg.getId(), intID,
                fg.getOwningCoflowID(), fg.getSrcLocation(), fg.getDstLocation(),
                fg.getTotalVolume() - fg.getTransmitted(), fg.filename, fg.flowInfos);

//        fgo.setRemainingVolume( fg.getTotalVolume()-fg.getTransmitted_agg() );

        return fgo;
    }*/

//    // newer version of converter
//    @Deprecated
//    public FlowGroup_Old_Compressed toFlowGroup_Old(int intID) {
//        FlowGroup_Old_Compressed fgo = new FlowGroup_Old_Compressed(this, intID);
//
//        return fgo;
//    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public boolean isSendingFinished() {
        return isSendingFinished;
    }

    public FlowGroupState getFlowGroupState() {
        return flowGroupState;
    }

    public FlowGroup setFlowGroupState(FlowGroupState flowGroupState) {
        this.flowGroupState = flowGroupState;
        return this;
    }

}
