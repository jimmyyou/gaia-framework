package gaiaframework.gaiaagent;

// FlowGroup Info contains the runtime information (remaining volume, rate etc.)
// it is created by SAs.

// Life Cycle: created ->

import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.util.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class AggFlowGroupInfo {

    final String ID;
    final double volume;

    volatile boolean finished = false;
    volatile double transmitted_agg = 0.0;

    AgentSharedData agentSharedData;

    public enum FlowState {
        INIT,
        RUNNING,
        PAUSED,
        FIN
    }

    volatile FlowState flowState = FlowState.INIT;

    // FIXME concurrency???
    // do not need the subscriptionInfo HashMap, only need this!
    public static class WorkerInfo {
        int pathID;
        String raID;
        double rate;

        public WorkerInfo(String raID, int pathID, double rate) {
            this.raID = raID;
            this.pathID = pathID;
            this.rate = rate;
        }

        public double getRate() { return rate; }

        public int getPathID() { return pathID; }

        public String getRaID() {
            return raID;
        }
    }

    // Maintain a list of which Worker is working on this
    List<WorkerInfo> workerInfoList = new ArrayList<>();

    // On Agg level, we only have FAID.
    String srcID;
    String dstFAID;


    // Below are fields for individual flows.
    List<FlowGroupInfo> flowGroupInfoList;


    public LinkedBlockingQueue<DataChunk> getDataQueue() {
        return dataQueue;
    }
    LinkedBlockingQueue<DataChunk> dataQueue; // not used anymore.
    Thread fileReader;

    public AggFlowGroupInfo(String ID, double volume) {
        this.ID = ID;
        this.volume = volume;
        this.finished = false;
    }

    public AggFlowGroupInfo(AgentSharedData agentSharedData, String afgID, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fue, String saID, String faID) {
        this.ID = afgID;
        this.volume = fue.getRemainingVolume();
        this.dstFAID = faID;
        this.srcID = saID;
        this.agentSharedData = agentSharedData;

        this.flowGroupInfoList = new ArrayList<>();

        // TODO put the content into the list
        addFlowInfo(flowGroupInfoList, fue);
    }

    private void addFlowInfo(List<FlowGroupInfo> flowGroupInfoList, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fue) {

        assert (flowGroupInfoList != null);

        for (int i = 0; i < fue.getFlowInfosCount(); i++) {
            // FIXME MUST FIX THIS. deleted due to protobuf change. (don't need this, only needs FlowInfo proto)
//            flowGroupInfoList.add(new FlowGroupInfo(this, fue.getFlowInfos(i), fue.getSrcIP(i), fue.getDstIP(i)));
        }

    }
/*
    public AggFlowGroupInfo(String ID, double volume, String fileName) {
        this.ID = ID;
        this.volume = volume;
        this.finished = false;
        this.dataQueue = new LinkedBlockingQueue<>();
        this.fileReader = new Thread( new LocalFileReader(fileName, dataQueue, (long) (volume * 1024 * 1024)));
        fileReader.start();
    }*/

    public void addWorkerInfo(String raID, int pathID, double rate) {
        workerInfoList.add(new AggFlowGroupInfo.WorkerInfo(raID, pathID, rate));
    }

    public void removeAllWorkerInfo() {
        workerInfoList.clear();
    }

    public String getID() {
        return ID;
    }

    public boolean isFinished() {
        return finished;
    }

    public double getVolume() {
        return volume;
    }

    // subscribed to ? who knows?
    public double getTransmitted_agg() {
        return transmitted_agg;
    }

    public synchronized boolean transmit(double v) {
        if (finished) { // first check if is already finished.
            return true;
        }
        transmitted_agg += v; // so volatile is not enough!
        if (transmitted_agg + Constants.DOUBLE_EPSILON >= volume) {
            finished = true;
            transmitted_agg = volume; // ensure that remaining volume >= 0
            return true;
        } else {
            return false;
        }
    }

    public FlowState getFlowState() {
        return flowState;
    }

    public AggFlowGroupInfo setFlowState(FlowState flowState) {
        this.flowState = flowState;
        return this;
    }
}
