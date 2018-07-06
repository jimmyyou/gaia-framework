package gaiaframework.network;

import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.gaiamaster.Coflow;
import gaiaframework.gaiamaster.FlowGroup;

import java.util.ArrayList;
import java.util.List;

// This is actually a FlowGroup, owned by a CoFlow.
public class FlowGroup_Old_Compressed {
    private String id;
    private int int_id;
    private String coflow_id; // id of owning coflow
    private String src_loc;
    private String dst_loc;
    private double remainingVolume;
    @Deprecated
    private double transmitted_volume; // It will always be zero
    @Deprecated
    private double rate; // in Mbps
    public ArrayList<Pathway> paths = new ArrayList<Pathway>();
    private boolean done = false;
    private long start_timestamp = -1;
    private long end_timestamp = -1;
    private boolean updated = false; // whether the flow has had its allocation updated
//    private boolean started_sending = false; // whether the flow has started to be sent
                                             // by a sending agent (only used by baseline)

    public List<ShuffleInfo.FlowInfo> flowInfos;
    public List<String> srcIPs;
    public List<String> dstIPs;

    // TODO change this, after compression there is no single file
    private String filename;

    public Coflow cf;
    public List<FlowGroup> fgList;

    // New constructor
    public FlowGroup_Old_Compressed(FlowGroup fg, int intID) {
        this.id = fg.getId();
        this.int_id = intID;
        this.coflow_id = fg.getOwningCoflowID();
        this.src_loc = fg.getSrcLocation();
        this.dst_loc = fg.getDstLocation();
        this.remainingVolume = fg.getTotalVolume() - fg.getTransmitted();
        this.rate = (double)0.0;
        this.transmitted_volume = (double)0.0;
        this.flowState = FlowState.INIT;
        this.filename = fg.getFilename();
        this.flowInfos = fg.flowInfos;

        this.srcIPs = fg.srcIPs;
        this.dstIPs = fg.dstIPs;
    }

    // the state of this flow
    public enum FlowState{
        INIT,
        STARTING,
        PAUSING,
        CHANGING
    }

    public String getFilename() {
        return filename;
    }

    private FlowState flowState;

    public FlowGroup_Old_Compressed(String id, int int_id, Coflow coflow, String src_loc, String dst_loc, double remainingVolume, List<FlowGroup> fgList) {
        this.id = id;
        this.int_id = int_id;
        this.cf = coflow;
        this.coflow_id = coflow.getId();
        this.src_loc = src_loc;
        this.dst_loc = dst_loc;
        this.remainingVolume = remainingVolume;
        this.rate = (double)0.0;
        this.transmitted_volume = (double)0.0;
        this.flowState = FlowState.INIT;
        this.fgList = fgList;
    }

    public FlowGroup_Old_Compressed(String id, int int_id, String coflow_id, String src_loc, String dst_loc, double remainingVolume,
                                    String filename, List<ShuffleInfo.FlowInfo> flowInfos) {
        this.id = id;
        this.int_id = int_id;
        this.coflow_id = coflow_id;
        this.src_loc = src_loc;
        this.dst_loc = dst_loc;
        this.remainingVolume = remainingVolume;
        this.rate = (double)0.0;
        this.transmitted_volume = (double)0.0;
        this.flowState = FlowState.INIT;
        this.filename = filename;
        this.flowInfos = flowInfos;
    }

    @Deprecated // Use getRemainingVolume()
    public double remaining_volume() {
        return remainingVolume - transmitted_volume;
    }

    ////// getters and setters /////
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getInt_id() {
        return int_id;
    }

    public void setInt_id(int int_id) {
        this.int_id = int_id;
    }

    public String getCoflow_id() {
        return coflow_id;
    }

    public void setCoflow_id(String coflow_id) {
        this.coflow_id = coflow_id;
    }

    public String getSrc_loc() {
        return src_loc;
    }

    public void setSrc_loc(String src_loc) {
        this.src_loc = src_loc;
    }

    public String getDst_loc() {
        return dst_loc;
    }

    public void setDst_loc(String dst_loc) {
        this.dst_loc = dst_loc;
    }

    public double getRemainingVolume() {
        return remainingVolume;
    }

    public void setRemainingVolume(double remainingVolume) {
        this.remainingVolume = remainingVolume;
    }

    public double getTransmitted_volume() {
        return transmitted_volume;
    }

    public void setTransmitted_volume(double transmitted_volume) {
        this.transmitted_volume = transmitted_volume;
    }

    @Deprecated
    public double getRate() {
        return rate;
    }

    @Deprecated
    public void setRate(double rate) {
        this.rate = rate;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }

    public long getStart_timestamp() {
        return start_timestamp;
    }

    public void setStart_timestamp(long start_timestamp) {
        this.start_timestamp = start_timestamp;
    }

    public long getEnd_timestamp() {
        return end_timestamp;
    }

    public void setEnd_timestamp(long end_timestamp) {
        this.end_timestamp = end_timestamp;
    }

    public boolean isUpdated() {
        return updated;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }

//    public boolean isStarted_sending() {
//        return started_sending;
//    }
//
//    public void setStarted_sending(boolean started_sending) {
//        this.started_sending = started_sending;
//    }

    public FlowState getFlowState() { return flowState; }

    public FlowGroup_Old_Compressed setFlowState(FlowState flowState) { this.flowState = flowState; return this; }
};
