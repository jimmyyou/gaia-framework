package gaiaframework.gaiaagent;

// This class tracks the FlowInformation for sending Agent.
// worker looks up information from here

import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.transmission.DataChunkMessage;

import java.util.concurrent.LinkedBlockingQueue;

public class FlowGroupInfo {

    String MapAttemptID;
    String ReduceAttemptID;
    String fileName;

    // TODO pass in this information!
    String srcHostIP;
    String dstHostIP;

    AggFlowGroupInfo parentFlowInfo;

    volatile double rate;

    AgentSharedData agentSharedData;


    LinkedBlockingQueue<DataChunkMessage> dataQueue;
    Thread fetcher;

    public FlowGroupInfo(AggFlowGroupInfo parent, ShuffleInfo.FlowInfo flowInfo, String srcIP, String dstIP) {
        this.fileName = flowInfo.getDataFilename();
        this.MapAttemptID = flowInfo.getMapAttemptID();
        this.ReduceAttemptID = flowInfo.getReduceAttemptID();
        this.parentFlowInfo = parent;

        this.srcHostIP = srcIP;
        this.dstHostIP = dstIP;

        this.agentSharedData = parent.agentSharedData;

        this.dataQueue = new LinkedBlockingQueue<>();

        fetcher = new Thread(new RemoteHTTPFetcher(this, flowInfo, dataQueue, srcHostIP, dstHostIP));

        fetcher.start();

    }

    public LinkedBlockingQueue<DataChunkMessage> getDataQueue() {
        return dataQueue;
    }


}
