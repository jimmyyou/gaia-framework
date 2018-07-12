package gaiaframework.gaiamaster;

// The new coflow definition. used by GAIA master, YARN emulator etc.

import com.google.errorprone.annotations.DoNotCall;
import gaiaframework.network.Coflow_Old_Compressed;
import gaiaframework.network.FlowGroup_Old_Compressed;
import gaiaframework.util.Constants;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Coflow {
    // final fields
    private final String id;
    private final double totalVolume;
    CountDownLatch isDoneLatch;
    public CountDownLatch isSmallFlowDoneLatch;
    CountDownLatch isCoflowFileFinishedLatch;

    // list of flowgroups: final? ArrayList or ConcurrentHashMap?
    private HashMap<String, FlowGroup> flowGroups;

    HashMap<String, FlowGroup> smallFlows;

    // multiple FGs may finish concurrently leading to the finish of Coflow.
    private AtomicBoolean finished = new AtomicBoolean(false);

    private long startTime = -1;

    private long endTime = -1;

    public int ddl_Millis = -1;

    public Coflow(String id, HashMap<String, FlowGroup> flowGroups) {
        this.id = id;
        this.flowGroups = flowGroups;
        this.isDoneLatch = new CountDownLatch(1);
        this.isCoflowFileFinishedLatch = new CountDownLatch(flowGroups.size());
        this.totalVolume = flowGroups.values().stream().mapToDouble(FlowGroup::getTotalVolume).sum();
    }

    /**

     * Create a Coflow with FlowGroups, and small flows (flows that are sent directly, without scheduling)
     *
     * @param id
     * @param flowGroups
     * @param smallFlows
     */
    @Deprecated
    public Coflow(String id, HashMap<String, FlowGroup> flowGroups, HashMap<String, FlowGroup> smallFlows) {
        this.id = id;
        this.flowGroups = flowGroups;
        this.smallFlows = smallFlows;
        this.isDoneLatch = new CountDownLatch(1);
        this.isSmallFlowDoneLatch = new CountDownLatch(smallFlows.size());
        this.totalVolume = flowGroups.values().stream().mapToDouble(FlowGroup::getTotalVolume).sum() + smallFlows.values().stream().mapToDouble(FlowGroup::getTotalVolume).sum();
    }

    public String getId() {
        return id;
    }


    public HashMap<String, FlowGroup> getFlowGroups() {
        return flowGroups;
    }

    public FlowGroup getFlowGroup(String fgid) {
        return flowGroups.get(fgid);
    }

    // TODO verify the two converters
    // converter between Old Coflow and new coflow, for use by Scheduler.
    // scheduler takes in ID, flowgroups (with IntID, srcLoc, dstLoc, volume remain.)
    public static Coflow_Old_Compressed toCoflow_Old_Compressed_with_Trimming(Coflow cf) {
        Coflow_Old_Compressed ret = new Coflow_Old_Compressed(cf.getId(), new String[]{"null"}); // location not specified here.

        HashMap<String, FlowGroup_Old_Compressed> flows = new HashMap<String, FlowGroup_Old_Compressed>();
        List<FlowGroup> fgToCompress = new LinkedList<>();

        int cnt = 0;
        for (FlowGroup fg : cf.getFlowGroups().values()) {
            if (fg.isSendingFinished() || fg.getTransmitted() + Constants.DOUBLE_EPSILON >= fg.getTotalVolume()) {
                continue;                // Trim the Coflow_Old_Compressed, so we don't schedule FGs that are already finished.
            }

            fgToCompress.add(fg);

        }

        Map<FlowGroup_Old_Compressed, List<FlowGroup>> compressedListMap = new HashMap<>();
        Map<String, List<FlowGroup>> fgbySrcLoc = fgToCompress.stream().collect(Collectors.groupingBy(FlowGroup::getSrcLocation));

        for (Map.Entry<String, List<FlowGroup>> fgSrcE : fgbySrcLoc.entrySet()) {
            String srcLoc = fgSrcE.getKey();

            Map<String, List<FlowGroup>> fgSrcEbyDstLoc = fgSrcE.getValue().stream().collect(Collectors.groupingBy(FlowGroup::getDstLocation));

            for (Map.Entry<String, List<FlowGroup>> fge : fgSrcEbyDstLoc.entrySet()) {
                String dstLoc = fge.getKey();
                List<FlowGroup> fgeList = fge.getValue();

                if (!fgeList.isEmpty()) {
                    double totalVolume = fgeList.stream().mapToDouble(FlowGroup::getRemainingVolume).sum();
                    String fgoID = cf.id + "_" + cnt;

//                    String id, int int_id, String coflow_id, String src_loc, String dst_loc, double volume

                    FlowGroup_Old_Compressed fgo = new FlowGroup_Old_Compressed(fgoID, cnt, cf, srcLoc, dstLoc, totalVolume, fgeList);

                    compressedListMap.put(fgo, fgeList);
                    flows.put(fgoID, fgo);

                    cnt++;
                }
            }
        }

//        FlowGroup_Old_Compressed fgo = fg.toFlowGroup_Old((cnt++));
//        flows.put(fg.getId(), fgo);

        ret.flows = flows;
        ret.ddl_Millis = cf.ddl_Millis;

        return ret;
    }
/*    public Coflow (Coflow_Old_Compressed cfo){
        this.id = cfo.id;
        this.aggFlowGroups = new HashMap<String , FlowGroup>();
        for(Map.Entry<String, FlowGroup_Old_Compressed> entry : cfo.flows.entrySet()){
            FlowGroup fg = new FlowGroup(entry.getValue());
            aggFlowGroups.put( fg.getId() , fg);
        }

    }*/

    public boolean getFinished() {
        return finished.get();
    }

//    public void setFinished(boolean value) { this.finished.set(value); }

    public boolean finish(boolean newValue) {

        if (isDoneLatch.getCount() != 0) {
            isDoneLatch.countDown();
        }

        return this.finished.getAndSet(newValue);
    }




    @Deprecated @DoNotCall
    public void finishSF() {
        if (isSmallFlowDoneLatch.getCount() != 0) {
            isSmallFlowDoneLatch.countDown();
        }
    }

    public void blockTillFinish() throws InterruptedException {
        isDoneLatch.await();
        isCoflowFileFinishedLatch.await();
    }

//    public boolean getAndSetFinished(boolean newValue) { return this.finished.getAndSet(newValue); }

    //    private int state;
//    private String owningClient;
// Optional field
    public long getStartTime() {
        return startTime;
    }

    public double getTotalVolume() { return totalVolume;}

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String toPrintableString() {
        StringBuilder sb = new StringBuilder();

        sb.append(" Coflow: ").append(this.id);
        for (FlowGroup fg : this.flowGroups.values()) {

            sb.append("\nFGID: ").append(fg.getId()).append(' ');
            sb.append("\nVolume: ").append(fg.getTotalVolume()).append(' ');
            sb.append("\nFile: ").append(fg.getFilename()).append('\n');
        }

        return sb.toString();
    }
}
