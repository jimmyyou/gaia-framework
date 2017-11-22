package gaiaframework.gaiaagent;

// This class tracks the FlowInformation for sending Agent.
// worker looks up information from here

import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.transmission.DataChunkMessage;

import java.util.ArrayList;
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


    public void transmit(long volume) {

        ArrayList<AggFlowGroupInfo> to_remove = new ArrayList<>();

        boolean done = parentFlowInfo.transmit(volume);

        if (done) { // meaning parent AggFlowGroup is done.

            //
            // wait until GAIA told us to stop, then stop. (although might cause a problem here.)

            to_remove.add(parentFlowInfo);
        }

        for (AggFlowGroupInfo afgi : to_remove) {

            String afgID = afgi.getID(); // fgID == fgiID

            agentSharedData.aggFlowGroups.remove(afgi.getID());

            // maybe not needing to delete here?
//            agentSharedData.subscriptionRateMaps.get(faID).get(pathID).remove(afgID);

            agentSharedData.finishFlow(afgID);


        }
    }
}
