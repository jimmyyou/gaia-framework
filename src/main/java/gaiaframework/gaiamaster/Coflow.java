package gaiaframework.gaiamaster;

// The new coflow definition. used by GAIA master, YARN emulator etc.

import gaiaframework.network.Coflow_Old;
import gaiaframework.network.FlowGroup_Old;
import gaiaframework.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class Coflow {
    // final fields
    private final String id;
    CountDownLatch isDoneLatch = new CountDownLatch(1);

    // list of flowgroups: final? ArrayList or ConcurrentHashMap?
    private HashMap<String , FlowGroup> flowGroups;

    // multiple FGs may finish concurrently leading to the finish of Coflow.
    private AtomicBoolean finished = new AtomicBoolean(false);

    private long startTime = -1;

    private long endTime = -1;

    public int ddl_Millis = -1;

    public Coflow(String id, HashMap<String , FlowGroup> flowGroups) {
        this.id = id;
        this.flowGroups = flowGroups;
    }

    public String getId() { return id; }


    public HashMap<String , FlowGroup>  getFlowGroups() { return flowGroups; }

    public FlowGroup getFlowGroup(String fgid) { return flowGroups.get(fgid); }

    // TODO verify the two converters
    // converter between Old Coflow and new coflow, for use by Scheduler.
    // scheduler takes in ID, flowgroups (with IntID, srcLoc, dstLoc, volume remain.)
    public static Coflow_Old toCoflow_Old_with_Trimming(Coflow cf){
        Coflow_Old ret = new Coflow_Old(cf.getId(), new String[]{"null"}); // location not specified here.

        HashMap<String, FlowGroup_Old> flows = new HashMap<String, FlowGroup_Old>();

        int cnt = 0;
        for (FlowGroup fg : cf.getFlowGroups().values()){
            if(fg.isFinished() || fg.getTransmitted() + Constants.DOUBLE_EPSILON >= fg.getTotalVolume())
            {
                continue;                // Trim the Coflow_Old, so we don't schedule FGs that are already finished.
            }

//            FlowGroup_Old fgo = FlowGroup.toFlowGroup_Old(fg, (cnt++));
            FlowGroup_Old fgo = fg.toFlowGroup_Old((cnt++));
            flows.put( fg.getId() , fgo);
        }

        ret.flows = flows;

        ret.ddl_Millis = cf.ddl_Millis;

        return ret;
    }
/*    public Coflow (Coflow_Old cfo){
        this.id = cfo.id;
        this.aggFlowGroups = new HashMap<String , FlowGroup>();
        for(Map.Entry<String, FlowGroup_Old> entry : cfo.flows.entrySet()){
            FlowGroup fg = new FlowGroup(entry.getValue());
            aggFlowGroups.put( fg.getId() , fg);
        }

    }*/

    public boolean getFinished() { return finished.get(); }

//    public void setFinished(boolean value) { this.finished.set(value); }

    public boolean finish(boolean newValue) {

        if (isDoneLatch.getCount() != 0 ){
            isDoneLatch.countDown();
        }

        return this.finished.getAndSet(newValue);
    }

    public void blockTillFinish() throws InterruptedException {
        isDoneLatch.await();
    }

//    public boolean getAndSetFinished(boolean newValue) { return this.finished.getAndSet(newValue); }

    //    private int state;
//    private String owningClient;
// Optional field
    public long getStartTime() { return startTime; }

    public void setStartTime(long startTime) { this.startTime = startTime; }

    public long getEndTime() { return endTime; }

    public void setEndTime(long endTime) { this.endTime = endTime; }

    public String toPrintableString(){
        StringBuilder sb = new StringBuilder();

        sb.append(" Coflow: ").append(this.id);
        for(FlowGroup fg : this.flowGroups.values()){

            sb.append("\nFGID: ").append(fg.getId()).append(' ');
            sb.append("\nVolume: ").append(fg.getTotalVolume()).append(' ');
            sb.append("\nFile: ").append(fg.getFilename()).append('\n');
        }

        return sb.toString();
    }
}
