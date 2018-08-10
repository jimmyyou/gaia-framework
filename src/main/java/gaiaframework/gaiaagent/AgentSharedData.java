package gaiaframework.gaiaagent;

// This is the data shared between the workers, inside the Sending Agent


import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.network.NetGraph;
import gaiaframework.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AgentSharedData {
    private static final Logger logger = LogManager.getLogger();

    final String saID;
    final String saName; // the name of Data Center in the trace file.
    public volatile int MAX_ACTIVE_CONNECTION;
    public ConcurrentHashMap<String, CountDownLatch> dstFilenameToLatchMap = new ConcurrentHashMap<>();


    enum SAState {
        IDLE, CONNECTING, READY
    }

    SAState saState = SAState.IDLE;

    CountDownLatch readySignal = new CountDownLatch(1);

    AtomicBoolean isSendingHeartBeat = new AtomicBoolean(false);

    CountDownLatch cnt_StartedConnections = null;
    final AtomicInteger activeConnections = new AtomicInteger(0);

    LinkedBlockingQueue<GaiaMessageProtos.FlowUpdate> fumQueue = new LinkedBlockingQueue<>();

    // moved the rpcClient to shared.
    AgentRPCClient rpcClient;

//    public HashMap<String, PConnection[]> connection_pools_ = new HashMap<String, PConnection[]>();

    // A Map of all Connections, indexed by PersistentConnection ID. PersistentConnection ID is
    // composed of ReceivingAgentID + PathID.
//    public HashMap<String, PConnection> connections_ = new HashMap<String, PConnection>();

    NetGraph netGraph;

    // TODO rethink about the data structures here. the consistency between the following two?

    // TODO do we need ConcurrentHashMap?
    // fgID -> FGI. FlowGroups that are currently being sent by this SendingAgent
//    public ConcurrentHashMap<String, AggFlowGroupInfo> aggFlowGroups = new ConcurrentHashMap<String, AggFlowGroupInfo>();
    public ConcurrentHashMap<String, FlowGroupInfo> flowGroupInfoConcurrentHashMap = new ConcurrentHashMap<>();

    // RAID , pathID -> FGID -> subscription info // ArrayList works good here!
    // TODO remove subscription mechanism, we should be able to directly set the rate and paths.
//    public HashMap<String, ArrayList<ConcurrentHashMap<String, SubscriptionInfo>>> subscriptionRateMaps = new HashMap<>();

            // faID , pathID -> workerQueue.
            HashMap<String, LinkedBlockingQueue<CTRL_to_WorkerMsg>[]> workerQueues = new HashMap<>();

//    public List< HashMap<String , SubscriptionInfo> > subscriptionRateMaps;


    public AgentSharedData(String saID, NetGraph netGraph) {
        this.saID = saID;
        this.saName = Constants.node_id_to_trace_id.get(saID);
        this.netGraph = netGraph;

/*//        IMPORTANT: initializing subscriptionRateMaps
        for (String ra_id : netGraph.nodes_) {
            if (!saID.equals(ra_id)) { // don't consider path to SA itself.
                // because apap is consistent among different programs.
                int pathSize = netGraph.apap_.get(saID).get(ra_id).size();
                ArrayList<ConcurrentHashMap<String, SubscriptionInfo>> maplist = new ArrayList<>(pathSize);
//                subscriptionRateMaps.put(ra_id, maplist);

                for (int i = 0; i < pathSize; i++) {
                    maplist.add(new ConcurrentHashMap<>());
                }
            }
        }*/

    }


    // Version 2.0 of finishFlow
    public void finishFlow(String fgID) {

        // null pointer because of double sending FG_FIN
        if (flowGroupInfoConcurrentHashMap.get(fgID) == null) {
            // already sent the TRANSFER_FIN message, do nothing
            return;
        }

/*        if (aggFlowGroups.get(fgID).getFlowState() == AggFlowGroupInfo.FlowState.FIN) {
            // already sent the TRANSFER_FIN message, do nothing
            logger.warn("Already sent the TRANSFER_FIN for {}", fgID);
            aggFlowGroups.remove(fgID);
            return;
        }*/

//        aggFlowGroups.get(fgID).setFlowState(AggFlowGroupInfo.FlowState.FIN);
        logger.info("Sending FLOW_FIN for {} to CTRL", fgID);
//        rpcClient.sendFG_FIN(fgID);
        pushFG_FIN(fgID);
        flowGroupInfoConcurrentHashMap.remove(fgID);
    }

    public void pushFG_FIN(String fgID) {
        if (fgID == null) {
            System.err.println("fgID = null when sending FG_FIN");
            return;
        }

        GaiaMessageProtos.FlowStatusReport.FlowStatus.Builder fsBuilder = GaiaMessageProtos.FlowStatusReport.FlowStatus.newBuilder()
                .setFinished(true).setId(fgID).setTransmitted(0);
//        GaiaMessageProtos.FlowStatusReport.Builder statusReportBuilder = GaiaMessageProtos.FlowStatusReport.newBuilder().addStatus(fsBuilder);
//        statusReportBuilder.addStatus(fsBuilder);

        GaiaMessageProtos.FlowStatusReport FG_FIN = GaiaMessageProtos.FlowStatusReport.newBuilder().addStatus(fsBuilder).build();

        // Call directly instead of using eventloop
        this.rpcClient.sendFlowStatus(FG_FIN);
//        logger.info("finished sending FLOW_FIN for {}", fgID);
    }


    public void pushStatusUpdate() {
        int size = flowGroupInfoConcurrentHashMap.size();
        if (size == 0) {
//            System.out.println("FG_SIZE = 0");
            return;         // if there is no data to send (i.e. the master has not come online), we simply skip.
        }

//        GaiaMessageProtos.FlowStatusReport statusReport = statusReportBuilder.build();
        GaiaMessageProtos.FlowStatusReport statusReport = buildCurrentFlowStatusReport();

        // Changed to directly call method
        this.rpcClient.sendFlowStatus(statusReport);

        logger.debug("finished pushing status report\n{}", statusReport);

//        while ( !isStreamReady ) {
//            initStream();
//            clientStreamObserver.onNext(statusReport);
//        }

    }

    /**
     * Update the aggFlowGroups and subscriptionRateMaps
     *
     * @param forwardingAgentID
     * @param fgID
     * @param fue
     */
    public void startFlowGroup(String forwardingAgentID, String fgID, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fue) {
        // add this flowgroup when not existent // only accept volume from CTRL at StartFG.
        logger.info("STARTING : {}", fgID);

        // HOWTO: create a Object FlowGroupInfo: containing flowInfo/progress, current rate, path
        if (!flowGroupInfoConcurrentHashMap.containsKey(fgID)) {
            logger.info("START failed: an existing flow {}", fgID);
            return;
        }
//        if (aggFlowGroups.containsKey(fgID)) {
//            logger.info("START failed: an existing flow {}", fgID);
//            return;
//        }

        FlowGroupInfo fgi = new FlowGroupInfo(forwardingAgentID, fgID, fue);
        flowGroupInfoConcurrentHashMap.put(fgID, fgi);

        // start the fetcher for this FG
        FlowGroupFetcher fgfetcher = new FlowGroupFetcher(fgi, this);
        fgfetcher.start();

//        AggFlowGroupInfo fgi = new AggFlowGroupInfo(fgID, fge.getRemainingVolume(), fge.getFilename()).setFlowGroupState(AggFlowGroupInfo.FlowGroupState.RUNNING);
//        AggFlowGroupInfo afgi = new AggFlowGroupInfo(this, fgID, fue, saID, faID).setFlowState(AggFlowGroupInfo.FlowState.RUNNING);
//        aggFlowGroups.put(fgID, afgi);

        // no need to subscribe
//        addAllSubscription(faID, fgID, fue, afgi);

    }


    public boolean changeFlowGroup(String faID, String fgID, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fge) {

        logger.info("CHANGING : {}", fgID);

        if (flowGroupInfoConcurrentHashMap.containsKey(fgID)) {
            FlowGroupInfo fgi = flowGroupInfoConcurrentHashMap.get(fgID);

            fgi.setRateLimiters(fge.getPathIDToRateMapMap());
            // TODO(future) we may need to store the state of the FG

            return true;
        } else {
            logger.trace("CHANGE/RESUME failed: a non-existing flow {}", fgID); // after FG finished, this can happen
            return false;
        }
    }

    public void pauseFlowGroup(String faID, String fgID, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fge) {

        if (flowGroupInfoConcurrentHashMap.containsKey(fgID)) {
            flowGroupInfoConcurrentHashMap.get(fgID).setPauseFlowGroup();
        } else {
            logger.error("PAUSE failed: a non-existing flow {}", fgID);
            return;
        }
    }

/*    public void printSAStatus() {

        StringBuilder strBuilder = new StringBuilder();
//        System.out.println("---------SA STATUS---------");
        strBuilder.append("---------SA STATUS---------\n");
        for (Map.Entry<String, AggFlowGroupInfo> fgie : aggFlowGroups.entrySet()) {
            AggFlowGroupInfo fgi = fgie.getValue();
            strBuilder.append(' ').append(fgi.getID()).append(' ').append(fgi.getFlowState()).append(' ').append(fgi.getVolume() - fgi.getTransmitted_agg()).append('\n');

            for (AggFlowGroupInfo.WorkerInfo wi : fgi.workerInfoList) {
                SubscriptionInfo tmpSI = subscriptionRateMaps.get(wi.getRaID()).get(wi.getPathID()).get(fgi.getID());
                strBuilder.append("  ").append(wi.getRaID()).append(' ').append(wi.getPathID()).append(' ').append(tmpSI.getRate()).append('\n');
            }

        }

        logger.info(strBuilder.toString());

    }*/

    public GaiaMessageProtos.FlowStatusReport buildCurrentFlowStatusReport() {

        GaiaMessageProtos.FlowStatusReport.Builder statusReportBuilder = GaiaMessageProtos.FlowStatusReport.newBuilder();

        for (Map.Entry<String, FlowGroupInfo> entry : flowGroupInfoConcurrentHashMap.entrySet()) {
            FlowGroupInfo fgi = entry.getValue();

            // Version 2.0
            GaiaMessageProtos.FlowStatusReport.FlowStatus.Builder fsBuilder = GaiaMessageProtos.FlowStatusReport.FlowStatus.newBuilder()
                    .setFinished(fgi.finished).setId(fgi.fgID).setTransmitted(fgi.totalVolume - fgi.remainingVolume);

            statusReportBuilder.addStatus(fsBuilder);
        }

        /*for (Map.Entry<String, AggFlowGroupInfo> entry : aggFlowGroups.entrySet()) {
            AggFlowGroupInfo afgi = entry.getValue();

            if (afgi.getFlowState() == AggFlowGroupInfo.FlowState.INIT) {
                logger.error("fgi in INIT state");
                continue;
            }
            if (afgi.getFlowState() == AggFlowGroupInfo.FlowState.FIN) {
                continue;
            }
            if (afgi.getFlowState() == AggFlowGroupInfo.FlowState.PAUSED) {
//                logger.info("");
                continue;
            }

//            if (fgi.getTransmitted_agg() == 0){
//                logger.info("FG {} tx=0, status {}",fgi.getID(), fgi.getFlowGroupState());
//                continue;
//            }

            GaiaMessageProtos.FlowStatusReport.FlowStatus.Builder fsBuilder = GaiaMessageProtos.FlowStatusReport.FlowStatus.newBuilder()
                    .setFinished(afgi.isFinished()).setId(afgi.getID()).setTransmitted(afgi.getTransmitted_agg());

            statusReportBuilder.addStatus(fsBuilder);
        }*/

        return statusReportBuilder.build();
    }

    /**
     * Sends msg to master after all FILE_FIN for FG have been processed.
     *
     * @param fgID
     */
    void pushFGFileAllFinished(String fgID) {
        rpcClient.sendFGFileFIN(fgID);
        logger.info("Finished sending FGFileFIN to master for {}", fgID);
    }

    /**
     * Process single FileFIN message
     *
     * @param dstFilename
     */
    void onSingleFILEFIN(String dstFilename) {

        // look up and count down the latch
        if (dstFilenameToLatchMap.containsKey(dstFilename)) {
            dstFilenameToLatchMap.get(dstFilename).countDown();
            // then delete the look up table
            dstFilenameToLatchMap.remove(dstFilename);
        } else {
            logger.error("Received FILEFIN for {}, but no latch found!", dstFilename);
        }
    }

/*    // Getters//

    public String getSaID() { return saID; }

    public String getSaName() { return saName; }

    public ConcurrentHashMap<String, FlowGroupInfo> getFlowGroups() { return aggFlowGroups; }*/
}
