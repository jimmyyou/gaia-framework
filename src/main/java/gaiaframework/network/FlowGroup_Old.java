package gaiaframework.network;

import edu.umich.gaialib.gaiaprotos.ShuffleInfo;

import java.util.ArrayList;
import java.util.List;

// This is actually a FlowGroup, owned by a CoFlow.
public class FlowGroup_Old {
    private String id;
    private int int_id;
    private String coflow_id; // id of owning coflow
    private String src_loc;
    private String dst_loc;
    private double volume;
    private double transmitted_volume;
    private double rate; // in Mbps
    public ArrayList<Pathway> paths = new ArrayList<Pathway>();
    private boolean done = false;
    private long start_timestamp = -1;
    private long end_timestamp = -1;
    private boolean updated = false; // whether the flow has had its allocation updated
//    private boolean started_sending = false; // whether the flow has started to be sent
                                             // by a sending agent (only used by baseline)

    public List<ShuffleInfo.FlowInfo> flowInfos;

    private String filename;

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

    public FlowGroup_Old(String id, int int_id, String coflow_id, String src_loc, String dst_loc, double volume) {
        this.id = id;
        this.int_id = int_id;
        this.coflow_id = coflow_id;
        this.src_loc = src_loc;
        this.dst_loc = dst_loc;
        this.volume = volume;
        this.rate = (double)0.0;
        this.transmitted_volume = (double)0.0;
        this.flowState = FlowState.INIT;
    }

    public FlowGroup_Old(String id, int int_id, String coflow_id, String src_loc, String dst_loc, double volume,
                         String filename, List<ShuffleInfo.FlowInfo> flowInfos) {
        this.id = id;
        this.int_id = int_id;
        this.coflow_id = coflow_id;
        this.src_loc = src_loc;
        this.dst_loc = dst_loc;
        this.volume = volume;
        this.rate = (double)0.0;
        this.transmitted_volume = (double)0.0;
        this.flowState = FlowState.INIT;
        this.filename = filename;
        this.flowInfos = flowInfos;
    }

    public double remaining_volume() {
        return volume - transmitted_volume;
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

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }

    public double getTransmitted_volume() {
        return transmitted_volume;
    }

    public void setTransmitted_volume(double transmitted_volume) {
        this.transmitted_volume = transmitted_volume;
    }

    public double getRate() {
        return rate;
    }

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

    public FlowGroup_Old setFlowState(FlowState flowState) { this.flowState = flowState; return this; }
};
