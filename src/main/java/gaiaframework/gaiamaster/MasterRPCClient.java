package gaiaframework.gaiamaster;


import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.gaiaprotos.SendingAgentServiceGrpc;
import gaiaframework.network.NetGraph;
import gaiaframework.network.Pathway;
import gaiaframework.scheduler.ScheduleOutputFG;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MasterRPCClient {

    private static final Logger logger = LogManager.getLogger();

    private final ManagedChannel channel;
    private final SendingAgentServiceGrpc.SendingAgentServiceBlockingStub blockingStub;
    private final SendingAgentServiceGrpc.SendingAgentServiceStub asyncStub;
    private StreamObserver<GaiaMessageProtos.FlowUpdate> fumStreamObserver;

    String targetIP;
    int targetPort;

    volatile boolean isStreamReady = false;

    public MasterRPCClient(String saIP, int saPort) {
        // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
        // needing certificates.
        this(ManagedChannelBuilder.forAddress(saIP, saPort).usePlaintext(true).build());
        this.targetIP = saIP;
        this.targetPort = saPort;
    }

    public MasterRPCClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = SendingAgentServiceGrpc.newBlockingStub(channel);
        asyncStub = SendingAgentServiceGrpc.newStub(channel);


    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void initStream() {
        logger.warn("(Re)starting the stream");
        StreamObserver<GaiaMessageProtos.FUM_ACK> FUMresponseObserver = new StreamObserver<GaiaMessageProtos.FUM_ACK>() {

            @Override
            public void onNext(GaiaMessageProtos.FUM_ACK fumAck) {
                logger.info("Received flowStatus_ack from server");
            }

            @Override
            public void onError(Throwable t) {
                logger.error("ERROR in sending FUM: {}", t.toString());
            }

            @Override
            public void onCompleted() {
                channel.shutdown();
            }
        };
        fumStreamObserver = asyncStub.changeFlow(FUMresponseObserver);

        isStreamReady = true;
    }

    public Iterator<GaiaMessageProtos.PAMessage> preparePConn() {
        GaiaMessageProtos.PAM_REQ req = GaiaMessageProtos.PAM_REQ.newBuilder().build();
        return blockingStub.prepareConnections(req);
    }


    public void startExp() {
        GaiaMessageProtos.Exp_CTRL hb = GaiaMessageProtos.Exp_CTRL.newBuilder().build();
        blockingStub.controlExperiment(hb);
    }

    /**
     * Sends flowGroupInfo to Agent, count down when finished.
     *
     * @param flowGroupList
     * @param downLatch
     */
    public void SetFlowInfoList(List<FlowGroup> flowGroupList, CountDownLatch downLatch) {

        GaiaMessageProtos.FlowGroupInfoBundle.Builder fgibBuilder = GaiaMessageProtos.FlowGroupInfoBundle.newBuilder();
        for (FlowGroup fg : flowGroupList) {

            GaiaMessageProtos.FlowGroupInfoMsg.Builder fgimBuilder = GaiaMessageProtos.FlowGroupInfoMsg.newBuilder();
            fgimBuilder.addAllFlowInfos(fg.flowInfos);
            fgimBuilder.setSrcLoc(fg.getSrcLocation());
            fgimBuilder.setDstLoc(fg.getDstLocation());
            fgimBuilder.setFgID(fg.getId());

            fgibBuilder.addFgimsg(fgimBuilder);
        }

        StreamObserver<GaiaMessageProtos.ACK> observer = new StreamObserver<GaiaMessageProtos.ACK>() {
            @Override
            public void onNext(GaiaMessageProtos.ACK ack) {

            }

            @Override
            public void onError(Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            @Override
            public void onCompleted() {
                downLatch.countDown();
                logger.info("Finish SetFlowInfoList for {}", targetIP);
            }
        };

        logger.info("Sending FlowInfoBundle to {}, content:\n{}", targetIP, fgibBuilder);
        asyncStub.setRecFlowInfoList(fgibBuilder.build(), observer);
    }

    /**
     * send FlowUpdateMessage, version 2.0.
     *
     * @param outputFGList
     * @param netGraph
     * @param saID
     * @param masterSharedData
     */
    public void setFlowNew(List<ScheduleOutputFG> outputFGList, NetGraph netGraph, String saID, MasterSharedData masterSharedData) {

        GaiaMessageProtos.FlowUpdate fum = buildFUMNew(outputFGList, netGraph, saID, masterSharedData);
        logger.info("Built the FUM\n {}", fum);

        if (!isStreamReady) {
            initStream();
        }

        fumStreamObserver.onNext(fum);
        logger.info("FUM sent ({} Byte) for saID = {}", fum.getSerializedSize(), saID);
    }

    private GaiaMessageProtos.FlowUpdate buildFUMNew(List<ScheduleOutputFG> outputFGList, NetGraph netGraph, String saID, MasterSharedData masterSharedData) {
        GaiaMessageProtos.FlowUpdate.Builder fumBuilder = GaiaMessageProtos.FlowUpdate.newBuilder();

        // first sort all fgos according to the RA.
        Map<String, List<ScheduleOutputFG>> fgobyRA = outputFGList.stream().collect(Collectors.groupingBy(ScheduleOutputFG::getDst_loc));

        for (Map.Entry<String, List<ScheduleOutputFG>> entrybyRA : fgobyRA.entrySet()) {

//            String raID = entrybyRA.getKey();

            GaiaMessageProtos.FlowUpdate.RAUpdateEntry.Builder raueBuilder = GaiaMessageProtos.FlowUpdate.RAUpdateEntry.newBuilder();
            raueBuilder.setRaID(entrybyRA.getKey());

            for (ScheduleOutputFG sofg : entrybyRA.getValue()) { // for each FGO of this RA, we create an FlowUpdateEntry
                assert (saID.equals(sofg.getSrc_loc()));
                String fgoID = sofg.getId();

                GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Builder fueBuilder = GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.newBuilder();
                fueBuilder.setFlowID(fgoID);

                switch (sofg.getFgoState()) {
                    case SCHEDULED:
                        logger.error("ERROR: FUM message contains flows that have not be make_path()");
                        continue; // skip this sofg.
//                        break;

                    case PAUSING:
                        fueBuilder.setOp(GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Operation.PAUSE);
                        break;

                    case STARTING:
                    case CHANGING:
                        if (sofg.getFgoState() == ScheduleOutputFG.FGOState.STARTING) {
                            fueBuilder.setOp(GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Operation.START);
                            // IMPORTANT: also send the List<FlowInfo> along with the START FUM
                            fueBuilder.addAllFlowInfos(masterSharedData.coflowPool.get(sofg.getCoflow_id()).getFlowGroup(sofg.getId()).flowInfos);
                        } else {
                            fueBuilder.setOp(GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Operation.CHANGE);
                        }

                        // FIXME: this message field is deprecated? We can tell after implementing agent.
                        fueBuilder.setRemainingVolume(masterSharedData.coflowPool.get(sofg.getCoflow_id()).getFlowGroup(sofg.getId()).getRemainingVolume());

                        for (Pathway p : sofg.paths) {
                            int pathID = netGraph.get_path_id(p);
                            if (pathID != -1) {
                                fueBuilder.putPathIDToRateMap(pathID, p.getBandwidth() * 1000000);
//                                fueBuilder.addPathToRate(GaiaMessageProtos.FlowUpdate.PathRateEntry.newBuilder().setPathID(pathID).setRate(p.getBandwidth() * 1000000));
                            } else {
                                logger.error("FATAL: illegal path for {}", sofg.getId());
//                                System.exit(1); // don't fail yet!
                            }
                        }

                        break;
                }

                raueBuilder.addFges(fueBuilder);
            } // end of creating all the FlowUpdateEntry

            fumBuilder.addRAUpdate(raueBuilder);
        } // end of creating all the RAUpdateEntry

        return fumBuilder.build();
    }
}
