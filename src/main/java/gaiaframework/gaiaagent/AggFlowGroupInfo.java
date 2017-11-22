package gaiaframework.gaiaagent;

// FlowGroup Info contains the runtime information (remaining volume, rate etc.)
// it is created by SAs.

// Life Cycle: created ->

import gaiaframework.util.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class AggFlowGroupInfo {

    final String ID;
    final double volume;

    volatile boolean finished = false;
    volatile double transmitted_agg = 0.0;

    public enum FlowState{
        INIT,
        RUNNING,
        PAUSED,
        FIN
    }

    volatile FlowState flowState = FlowState.INIT;

    // Below are fields for individual flows.
    List<FlowGroupInfo> flowGroupInfoList;

    // Maintain a list of which Worker is working on this
    List<WorkerInfo> workerInfoList = new ArrayList<>();

    public LinkedBlockingQueue<DataChunk> getDataQueue() {
        return dataQueue;
    }

    LinkedBlockingQueue<DataChunk> dataQueue;

    Thread fileReader;



    public class WorkerInfo{
        int pathID;
        String raID;

        public WorkerInfo(String raID, int pathID) {
            this.raID = raID;
            this.pathID = pathID;
        }

        public int getPathID() { return pathID; }

        public String getRaID() { return raID; }
    }

    public AggFlowGroupInfo(String ID, double volume) {
        this.ID = ID;
        this.volume = volume;
        this.finished = false;
    }

    public AggFlowGroupInfo(String ID, double volume, String fileName) {
        this.ID = ID;
        this.volume = volume;
        this.finished = false;
        this.dataQueue = new LinkedBlockingQueue<>();
        this.fileReader = new Thread( new LocalFileReader(fileName, dataQueue, (long) (volume * 1024 * 1024)));
        fileReader.start();
    }

    public void addWorkerInfo(String raID, int pathID){
        workerInfoList.add( new AggFlowGroupInfo.WorkerInfo(raID, pathID));
    }

    public void removeAllWorkerInfo(){
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
        if (finished){ // first check if is already finished.
            return true;
        }
        transmitted_agg += v; // so volatile is not enough!
        if (transmitted_agg + Constants.DOUBLE_EPSILON >= volume){
            finished = true;
            transmitted_agg = volume; // ensure that remaining volume >= 0
            return true;
        }
        else {
            return false;
        }
    }

    public FlowState getFlowState() { return flowState; }

    public AggFlowGroupInfo setFlowState(FlowState flowState) { this.flowState = flowState; return this;}
}
