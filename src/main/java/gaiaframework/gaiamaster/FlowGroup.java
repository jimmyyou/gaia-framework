package gaiaframework.gaiamaster;

// New definition of FlowGroup

import edu.umich.gaialib.FlowInfo;
import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.network.FlowGroup_Old;
import gaiaframework.network.Pathway;
import gaiaframework.util.Constants;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class FlowGroup {

    // new
    public List<ShuffleInfo.FlowInfo> flowInfos = new LinkedList<>();
    public List<String> srcIPs = new LinkedList<>();
    public List<String> dstIPs = new LinkedList<>();

    // final fields
    private final String id;
    private final String srcLocation;
    private final String dstLocation;
    private final String owningCoflowID;

    // non-final fields
    private long startTime = -1;
    private double totalVolume;
    private long endTime = -1;

    private boolean finished = false; // set true along with setting endTime.

    // make this field volatile! Or maybe atomic?
    private volatile double transmitted;

    String filename = null;
    String mapID;
    String redID;

    // the state of this flow
    public enum FlowState {
        NEW,
        RUNNING,
        PAUSED,
        FIN
    }

    private FlowState flowState;

    // The subflow info, is essientially immutable data? Nope.
    private ArrayList<Pathway> paths = new ArrayList<Pathway>();

    public String getFilename() {
        return filename;
    }

    public String getMapID() {
        return mapID;
    }

    public String getRedID() {
        return redID;
    }

    public FlowGroup(String id, String srcLocation, String dstLocation, String owningCoflowID, double totalVolume) {
        this.id = id;
        this.srcLocation = srcLocation;
        this.dstLocation = dstLocation;
        this.owningCoflowID = owningCoflowID;
        this.totalVolume = totalVolume;
        this.flowState = FlowState.NEW;
    }

    public FlowGroup(String id, String srcLocation, String dstLocation, String owningCoflowID, double totalVolume, String filename, String mapID, String redID) {
        this.id = id;
        this.srcLocation = srcLocation;
        this.dstLocation = dstLocation;
        this.owningCoflowID = owningCoflowID;
        this.totalVolume = totalVolume;
        this.flowState = FlowState.NEW;
        this.filename = filename;
        this.mapID = mapID;
        this.redID = redID;
    }

    // This method is called upon receiving Status Update, this method must be call if a Flow is finishing
    // if a flow is already marked finished, we don't invoke coflowFIN
    public synchronized boolean getAndSetFinish(long timestamp) {
        if (finished && this.transmitted + Constants.DOUBLE_EPSILON >= totalVolume) { // if already finished, do nothing
            return true;
        } else { // if we are the first thread to finish it
            this.transmitted = this.totalVolume;
            this.endTime = timestamp;
            this.finished = true;
            this.flowState = FlowState.FIN;
            return false;
        }
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

/*    public static FlowGroup_Old toFlowGroup_Old(FlowGroup fg, int intID) {
        FlowGroup_Old fgo = new FlowGroup_Old(fg.getId(), intID,
                fg.getOwningCoflowID(), fg.getSrcLocation(), fg.getDstLocation(),
                fg.getTotalVolume() - fg.getTransmitted(), fg.filename, fg.flowInfos);

//        fgo.setVolume( fg.getTotalVolume()-fg.getTransmitted_agg() );

        return fgo;
    }*/

    // newer version of converter
    public FlowGroup_Old toFlowGroup_Old(int intID) {
        FlowGroup_Old fgo = new FlowGroup_Old(this, intID);

        return fgo;
    }

    public FlowGroup(FlowGroup_Old fgo) {
        this.id = fgo.getId();
        this.srcLocation = fgo.getSrc_loc();
        this.dstLocation = fgo.getDst_loc();
        this.owningCoflowID = fgo.getCoflow_id();
        this.totalVolume = fgo.getVolume();
        this.transmitted = fgo.getTransmitted_volume();
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public boolean isFinished() {
        return finished;
    }

    public FlowState getFlowState() {
        return flowState;
    }

    public FlowGroup setFlowState(FlowState flowState) {
        this.flowState = flowState;
        return this;
    }

    public void addTotalVolume(long volume) {
        totalVolume += volume;
    }
}
