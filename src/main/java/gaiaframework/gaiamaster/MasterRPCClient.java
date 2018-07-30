package gaiaframework.gaiamaster;


import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.gaiaprotos.SendingAgentServiceGrpc;
import gaiaframework.network.FlowGroup_Old_Compressed;
import gaiaframework.network.NetGraph;
import gaiaframework.network.Pathway;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
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

    public void setFlow(Collection<FlowGroup_Old_Compressed> fgos, NetGraph ng, String saID) {

        GaiaMessageProtos.FlowUpdate fum = buildFUM(fgos, ng, saID);
        logger.info("Built the FUM\n {}", fum);

        if (!isStreamReady) {
            initStream();
        }

        fumStreamObserver.onNext(fum);
        logger.info("FUM sent ({} Byte) for saID = {}", fum.getSerializedSize(), saID);

    }


    public GaiaMessageProtos.FlowUpdate buildFUM(Collection<FlowGroup_Old_Compressed> fgos, NetGraph ng, String saID) {

        GaiaMessageProtos.FlowUpdate.Builder fumBuilder = GaiaMessageProtos.FlowUpdate.newBuilder();

        // first sort all fgos according to the RA.
        Map<String, List<FlowGroup_Old_Compressed>> fgobyRA = fgos.stream().collect(Collectors.groupingBy(FlowGroup_Old_Compressed::getDst_loc));

        for (Map.Entry<String, List<FlowGroup_Old_Compressed>> entrybyRA : fgobyRA.entrySet()) {

//            String raID = entrybyRA.getKey();

            GaiaMessageProtos.FlowUpdate.RAUpdateEntry.Builder raueBuilder = GaiaMessageProtos.FlowUpdate.RAUpdateEntry.newBuilder();
            raueBuilder.setRaID(entrybyRA.getKey());

            for (FlowGroup_Old_Compressed fgo : entrybyRA.getValue()) { // for each FGO of this RA, we create an FlowUpdateEntry
                assert (saID.equals(fgo.getSrc_loc()));
                String fgoID = fgo.getId();

                GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Builder fueBuilder = GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.newBuilder();
                fueBuilder.setFlowID(fgoID);

                if (fgo.getFlowState() == FlowGroup_Old_Compressed.FlowState.INIT) {
                    logger.error("ERROR: FUM message contains flows that have not been scheduled");
                    continue;
                } else if (fgo.getFlowState() == FlowGroup_Old_Compressed.FlowState.PAUSING) {
                    fueBuilder.setOp(GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Operation.PAUSE);
                } else if (fgo.getFlowState() == FlowGroup_Old_Compressed.FlowState.STARTING ||
                        fgo.getFlowState() == FlowGroup_Old_Compressed.FlowState.CHANGING) { // STARTING && CHANGING

                    if (fgo.getFlowState() == FlowGroup_Old_Compressed.FlowState.STARTING) {
                        fueBuilder.setOp(GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Operation.START);

                        // This is not the list of filenames, so should not be used here.
//                        fueBuilder.setFilename(fgo.getFilename()); // set the filename only for the start message

                        // also send the List<FlowInfo> along with the info
                        fueBuilder.addAllFlowInfos(fgo.flowInfos);

                        // also send the IP
                        // TODO integrate IP into fields of FlowInfo
                        fueBuilder.addAllSrcIP(fgo.srcIPs);
                        fueBuilder.addAllDstIP(fgo.dstIPs);

                    } else {
                        fueBuilder.setOp(GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Operation.CHANGE);
                    }

                    fueBuilder.setRemainingVolume(fgo.remaining_volume());
                    for (Pathway p : fgo.paths) {
                        int pathID = ng.get_path_id(p);
                        if (pathID != -1) {
                            fueBuilder.addPathToRate(GaiaMessageProtos.FlowUpdate.PathRateEntry.newBuilder().setPathID(pathID).setRate(p.getBandwidth() * 1000000));
                        } else {
                            System.err.println("FATAL: illegal path!");
//                    System.exit(1); // don't fail yet!
                        }
                    }
                }

                raueBuilder.addFges(fueBuilder);
            } // end of creating all the FlowUpdateEntry

            fumBuilder.addRAUpdate(raueBuilder);
        } // end of creating all the RAUpdateEntry

        return fumBuilder.build();
    }

    public void startExp() {
        GaiaMessageProtos.Exp_CTRL hb = GaiaMessageProtos.Exp_CTRL.newBuilder().build();
        blockingStub.controlExperiment(hb);
    }
}
