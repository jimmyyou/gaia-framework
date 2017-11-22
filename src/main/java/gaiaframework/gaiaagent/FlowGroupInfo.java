package gaiaframework.gaiaagent;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class FlowGroupInfo {

    // Maintain a list of which Worker is working on this AggFlow
    List<AggFlowGroupInfo.WorkerInfo> workerInfoList = new ArrayList<>();

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
}
