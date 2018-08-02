package gaiaframework.scheduler;

import gaiaframework.network.Pathway;

import java.util.LinkedList;

public class ScheduleOutputFG {
    private String id;
    private int int_id;
//    private String coflow_id; // id of owning coflow
    private String src_loc;
    private String dst_loc;
    public LinkedList<Pathway> paths = new LinkedList<Pathway>();

    public ScheduleOutputFG(CoflowScheduler.CoflowSchedulerEntry.FlowGroupSchedulerEntry fgse){
        this.id = fgse.fgID;
        this.int_id = fgse.intID;
        this.src_loc = fgse.srcLoc;
        this.dst_loc = fgse.dstLoc;
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
