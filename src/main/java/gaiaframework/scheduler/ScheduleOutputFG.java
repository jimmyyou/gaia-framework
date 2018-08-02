package gaiaframework.scheduler;

import gaiaframework.gaiamaster.FlowGroup;
import gaiaframework.network.Pathway;

import java.util.LinkedList;

public class ScheduleOutputFG {
    private String id;
//    private int int_id;
//    private String coflow_id; // id of owning coflow
    private String src_loc;
    private String dst_loc;
    public LinkedList<Pathway> paths = new LinkedList<Pathway>();

    // the state of this flow
    public enum FGOState{
        SCHEDULED,
        STARTING,
        PAUSING,
        CHANGING
    }

    private FGOState fgoState;

    public FGOState getFgoState() {
        return fgoState;
    }

    public void setFgoState(FGOState fgoState) {
        this.fgoState = fgoState;
    }

    public ScheduleOutputFG(CoflowScheduler.CoflowSchedulerEntry.FlowGroupSchedulerEntry fgse){
        this.id = fgse.fgID;
//        this.int_id = fgse.intID;
        this.src_loc = fgse.srcLoc;
        this.dst_loc = fgse.dstLoc;
        this.fgoState = FGOState.SCHEDULED;
    }

    public ScheduleOutputFG(String id, String src_loc, String dst_loc, FGOState fgoState) {
        this.id = id;
        this.src_loc = src_loc;
        this.dst_loc = dst_loc;
        this.fgoState = fgoState;
    }

    public String getSrc_loc() {
        return src_loc;
    }

    public String getDst_loc() {
        return dst_loc;
    }

    public String getId() {
        return id;
    }
}
