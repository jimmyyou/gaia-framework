package gaiaframework.gaiamaster;

import gaiaframework.comm.PortAnnouncementMessage_Old;
import gaiaframework.comm.PortAnnouncementRelayMessage;
import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.network.Coflow_Old_Compressed;
import gaiaframework.network.FlowGroup_Old_Compressed;
import gaiaframework.network.NetGraph;
import gaiaframework.network.Pathway;
import gaiaframework.scheduler.CoflowScheduler;
import gaiaframework.spark.YARNServer;
import gaiaframework.util.Configuration;
import gaiaframework.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

//@SuppressWarnings("Duplicates")

public class Master {
    private static final Logger logger = LogManager.getLogger();
    private final String outdir;
    private final Configuration config;
    private final CoflowScheduler scheduler;
    private final boolean is_SettingFlowRules;
    private final boolean isDebugMode;

    NetGraph netGraph;

    HashMap<String, MasterRPCClient> rpcClientHashMap;

    final MasterRPCServer rpcServer;

    MasterSharedData masterSharedData = new MasterSharedData();

    // SubModules of Master
    protected Thread coflowListener;
    //    protected Thread agentController; // This is similar to the Manager eventloop in old version. // maybe multiple threads?
    protected final ScheduledExecutorService mainExec; // For periodic call of schedule()

    protected final ExecutorService saControlExec;

    protected YARNServer yarnServer;


    public Master(String gml_file, String scheduler_type, String outdir, String configFile,
                  double bw_factor, boolean isSettingFlowRules, boolean isDebugMode) throws IOException {

        this.outdir = outdir;
        this.netGraph = new NetGraph(gml_file, bw_factor);
        this.rpcClientHashMap = new HashMap<>();
        this.is_SettingFlowRules = isSettingFlowRules;
        this.isDebugMode = isDebugMode;

        printPaths(netGraph);
        if (configFile == null) {
            this.config = new Configuration(netGraph.nodes_.size());
        } else {
            this.config = new Configuration(netGraph.nodes_.size(), configFile);
        }

        this.rpcServer = new MasterRPCServer(this.config, this.masterSharedData);

        this.mainExec = Executors.newScheduledThreadPool(1);

        // setting up the scheduler
        scheduler = new CoflowScheduler(netGraph);

        if (scheduler_type.equals("baseline")) { // no baseline!!!
            System.err.println("No baseline");
            System.exit(1);
//            scheduler = new BaselineScheduler(netGraph);
//            enablePersistentConn = false;
        } else if (scheduler_type.equals(Constants.SCHEDULER_NAME_GAIA)) {
//            scheduler = new PoorManScheduler(netGraph);
//            scheduler = new CoflowScheduler(netGraph);
//            enablePersistentConn = true;
            System.out.println("Using coflow scheduler");
        } else {
            System.out.println("Unrecognized scheduler type: " + scheduler_type);
            System.out.println("Scheduler must be one of { baseline, gaia }");
            System.exit(1);
        }

        saControlExec = Executors.newFixedThreadPool(netGraph.nodes_.size());
    }


    public void emulate() {
        LinkedBlockingQueue<PortAnnouncementMessage_Old> PAEventQueue = new LinkedBlockingQueue<PortAnnouncementMessage_Old>();

        logger.info("Starting master RPC server");

        // first start the server to receive status update
        MasterRPCServer rpcServer = new MasterRPCServer(config, this.masterSharedData);
        try {
            rpcServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }


        logger.info("Starting master RPC Client");

        // we have netGraph.nodes_.size() SAs
        for (String sa_id : netGraph.nodes_) {
            int id = Integer.parseInt(sa_id); // id is from 0 to n, IP from 1 to (n+1)
            MasterRPCClient client = new MasterRPCClient(config.getSAIP(id), config.getSAPort(id));
            rpcClientHashMap.put(sa_id, client);
            Iterator<GaiaMessageProtos.PAMessage> it = client.preparePConn();
            while (it.hasNext()) {
                GaiaMessageProtos.PAMessage pam = it.next();
//                System.out.print(pam);
                try {
                    PAEventQueue.put(new PortAnnouncementMessage_Old(pam.getSaId(), pam.getRaId(), pam.getPathId(), pam.getPortNo()));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

//            sai.put(sa_id, new SendingAgentInterface(sa_id, netGraph, config.getSAIP(id), config.getSAPort(id), PAEventQueue, this.ms , enablePersistentConn));
        }

        logger.info("Connection established between SA-RA");

        if (is_SettingFlowRules) {
            // receive the port announcements from SendingAgents and set appropriate flow rules.
            PortAnnouncementRelayMessage relay = new PortAnnouncementRelayMessage(netGraph, PAEventQueue);
            relay.relay_ports();
            logger.info("All flow rules set up sleeping 5s before starting HeartBeat");
        } else {
            logger.info("skipping setting flow rules, sleep 5s");
        }


        try {
            Thread.sleep(5000); // sleep 10s for the rules to propagate // TODO test the rules first before proceeding
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // start sending heartbeat
        logger.info("start heartbeat");
        for (String sa_id : netGraph.nodes_) {
            rpcClientHashMap.get(sa_id).startExp();
        }

        logger.info("All SA heartbeat started");

        // start the other two threads.
        coflowListener.start();

        System.out.println("Master: starting periodical scheduler at every " + Constants.SCHEDULE_INTERVAL_MS + " ms.");
        // start the periodic execution of schedule()

//        final Runnable runSchedule = () -> schedule();
        final Runnable runSchedule = () -> schedule();
        ScheduledFuture<?> mainHandler = mainExec.scheduleAtFixedRate(runSchedule, 0, Constants.SCHEDULE_INTERVAL_MS, MILLISECONDS);

        // Start the input
        yarnServer = new YARNServer(config, Constants.DEFAULT_YARN_PORT, masterSharedData, isDebugMode, this);
        try {
            yarnServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Master init finished, block the main thread");
        try {
            rpcServer.blockUntilShutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void simulate() {
        System.out.println("Simulation not supported");
        System.err.println("Simulation not supported in this version");
        System.exit(1);
    }

    // print the pathway with their ID
    private void printPaths(NetGraph ng) {
        for (String src : ng.nodes_) {
            if (ng.apap_.get(src) == null) {
                continue;
            }
            for (String dst : ng.nodes_) {
                ArrayList<Pathway> list = ng.apap_.get(src).get(dst);
                if (list != null && list.size() > 0) {
                    System.out.println("Paths for " + src + " - " + dst + " :");
                    for (int i = 0; i < list.size(); i++) {
                        System.out.println(i + " : " + list.get(i));
                    }
                }
            }
        }
    }

    // the new version of schedule()
    // 1. check in the last interval if anything happens, and determine a fast schedule or re-do the sorting process
    private void schedule() {
//        logger.info("schedule_New(): CF_ADD: {} CF_FIN: {} FG_FIN: {}", masterSharedData.flag_CF_ADD, masterSharedData.flag_CF_FIN, masterSharedData.flag_FG_FIN);

        long currentTime = System.currentTimeMillis();
        List<FlowGroup_Old_Compressed> scheduledFGOs = new ArrayList<>(0);
        List<FlowGroup_Old_Compressed> decompressedFGOsToSend = new ArrayList<>();

        // snapshoting and converting
        HashMap<String, Coflow_Old_Compressed> outcf = new HashMap<>();
        for (Map.Entry<String, Coflow> ecf : masterSharedData.coflowPool.entrySet()) {
            Coflow_Old_Compressed cfo = Coflow.toCoflow_Old_Compressed_with_Trimming(ecf.getValue());
            outcf.put(cfo.getId(), cfo);
        }

        // process Link change
        GaiaMessageProtos.PathStatusReport m = masterSharedData.linkStatusQueue.poll();
        while (m != null) {
            scheduler.processLinkChange(m);
            m = masterSharedData.linkStatusQueue.poll();

            // TODO need to trigger reschedule here
            masterSharedData.flag_CF_ADD = true;
        }

//        printCFList(outcf);

        if (masterSharedData.flag_CF_ADD) { // redo sorting, may result in preemption
            masterSharedData.flag_CF_ADD = false;
            masterSharedData.flag_CF_FIN = false;
            masterSharedData.flag_FG_FIN = false;

            // TODO update the CF_Status in scheduler
            scheduler.resetCFList(outcf);

//            scheduler.printCFList();

            try {
                scheduledFGOs = scheduler.scheduleRRF(currentTime);

                decompressedFGOsToSend = parseFlowState_DeCompress(masterSharedData, scheduledFGOs);
                sendControlMessages_Async(decompressedFGOsToSend);

            } catch (Exception e) {
                e.printStackTrace();
            }

        } else if (masterSharedData.flag_CF_FIN) { // no LP-sort, just update volume status and re-schedule
            masterSharedData.flag_CF_FIN = false;
            masterSharedData.flag_FG_FIN = false;
            scheduler.handleCoflowFIN(outcf);

//            scheduler.printCFList();

            try {
                scheduledFGOs = scheduler.scheduleRRF(currentTime);

                decompressedFGOsToSend = parseFlowState_DeCompress(masterSharedData, scheduledFGOs);
                sendControlMessages_Async(decompressedFGOsToSend);

            } catch (Exception e) {
                e.printStackTrace();
            }

        } else if (masterSharedData.flag_FG_FIN) { // no-reschedule, just pick up a new flowgroup.
            masterSharedData.flag_FG_FIN = false;
            scheduler.handleFlowGroupFIN(outcf);

//            scheduler.printCFList();

            try {
                scheduledFGOs = scheduler.scheduleRRF(currentTime);

                decompressedFGOsToSend = parseFlowState_DeCompress(masterSharedData, scheduledFGOs);
                sendControlMessages_Async(decompressedFGOsToSend);

            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {  // if none, NOP
            return; // no need to print out the execution time.
        }

        long deltaTime = System.currentTimeMillis() - currentTime;

        StringBuilder fgoContent = new StringBuilder("\n");
        for (FlowGroup_Old_Compressed fgo : decompressedFGOsToSend) {
            fgoContent.append(fgo.getId()).append(' ').append(fgo.paths).append(' ').append(fgo.getFlowState()).append('\n');
        }
        logger.info("FG content: {}", fgoContent);
        logger.info("schedule(): took {} ms. Active CF: {} Scheduled FG: {}/{}", deltaTime, masterSharedData.coflowPool.size(), scheduledFGOs.size(), decompressedFGOsToSend.size());

//        printMasterState();
    }

    // update the flowState in the CFPool, before sending out the information.
    private List<FlowGroup_Old_Compressed> parseFlowState_DeCompress(MasterSharedData masterSharedData, List<FlowGroup_Old_Compressed> scheduledCompressedFGs) {
        List<FlowGroup_Old_Compressed> fgoToSend = new ArrayList<>();
        HashMap<String, FlowGroup_Old_Compressed> fgoHashMap = new HashMap<>();

        StringBuilder fgoContent = new StringBuilder("\n");
        for (FlowGroup_Old_Compressed fgo : scheduledCompressedFGs) {
            fgoContent.append(fgo.getId()).append(' ').append(fgo.paths).append(' ').append(fgo.getFlowState()).append('\n');
        }
        logger.info("FGOs to decomp {}", fgoContent);

        // first decompress convert List to hashMap
        int cnt = 0;
        for (FlowGroup_Old_Compressed fgo : scheduledCompressedFGs) {

//            logger.info("Decompressing {}", fgo.getId());

            // decompress here
            for (FlowGroup decompressedFG : fgo.fgList) {

                FlowGroup_Old_Compressed fgo_decompressed = decompressedFG.toFlowGroup_Old(cnt++);
                // FIXME: need to make the ratio sum up to 1. Another way to achieve this is to remember the remaining volume when snapshoting.
//                logger.info("Decompress: {} : {} - {} / {}", decompressedFG.getId(), decompressedFG.getTotalVolume(), decompressedFG.getTransmitted(), fgo.getRemainingVolume());
                double ratio = (decompressedFG.getTotalVolume() - decompressedFG.getTransmitted()) / fgo.getRemainingVolume();

                // clone the paths, and set the BW
                fgo_decompressed.paths = new ArrayList<>();
                for (Pathway p : fgo.paths) {
                    Pathway decompressedPathway = new Pathway(p);
                    decompressedPathway.setBandwidth(p.getBandwidth() * ratio);
                    fgo_decompressed.paths.add(decompressedPathway);
                }
//                logger.info("Decompress: {} has {} of {}, paths: {}", decompressedFG.getId(), ratio, fgo.getId(), fgo_decompressed.paths);

//                fgoHashMap.put(fgo.getId(), fgo);
                fgoHashMap.put(decompressedFG.getId(), fgo_decompressed);
            }
        }

        // traverse all FGs in CFPool
        for (Map.Entry<String, Coflow> ecf : masterSharedData.coflowPool.entrySet()) { // snapshoting should not be a problem
            Coflow cf = ecf.getValue();
            for (Map.Entry<String, FlowGroup> fge : cf.getFlowGroups().entrySet()) {
                FlowGroup fg = fge.getValue();
                if (fg.getFlowGroupState() == FlowGroup.FlowGroupState.TRANSFER_FIN) {
                    // FIXME repeated too many times here. (Because FGs are skewed.)
//                    logger.info("find fg {} in TRANSFER_FIN state, to be ignored", fg.getId());
                    continue; // ignore finished, they shall be removed shortly
                } else if (fg.getFlowGroupState() == FlowGroup.FlowGroupState.RUNNING) { // may pause/change the running flow
                    if (fgoHashMap.containsKey(fg.getId())) { // we may need to change, if the path/rate are different TODO: speculatively send change message
                        fgoToSend.add(fgoHashMap.get(fg.getId()).setFlowState(FlowGroup_Old_Compressed.FlowState.CHANGING)); // running flow needs to change
                    } else { // we need to pause
                        fg.setFlowGroupState(FlowGroup.FlowGroupState.PAUSED);
                        fgoToSend.add(fg.toFlowGroup_Old(0).setFlowState(FlowGroup_Old_Compressed.FlowState.PAUSING));
//                        fgoToSend.add ( FlowGroup.toFlowGroup_Old(fg, 0).setFlowGroupState(FlowGroup_Old_Compressed.FlowGroupState.PAUSING) );
                    }
                } else { // case: NEW/PAUSED
                    if (fgoHashMap.containsKey(fg.getId())) { // we take action only if the flow get (re)scheduled
                        if (fg.getFlowGroupState() == FlowGroup.FlowGroupState.NEW) { // start the flow
                            fg.setFlowGroupState(FlowGroup.FlowGroupState.RUNNING);
                            fgoToSend.add(fgoHashMap.get(fg.getId()).setFlowState(FlowGroup_Old_Compressed.FlowState.STARTING));
                            masterSharedData.flowStartCnt++;
                        } else if (fg.getFlowGroupState() == FlowGroup.FlowGroupState.PAUSED) { // RESUME the flow
                            fg.setFlowGroupState(FlowGroup.FlowGroupState.RUNNING);
                            fgoToSend.add(fgoHashMap.get(fg.getId()).setFlowState(FlowGroup_Old_Compressed.FlowState.CHANGING));
                        }

                    }
                }
            }

        }

        return fgoToSend;
    }

    private void sendControlMessages_Async(List<FlowGroup_Old_Compressed> scheduledFGs) {
        // group FGOs by SA
        Map<String, List<FlowGroup_Old_Compressed>> fgoBySA = scheduledFGs.stream()
                .collect(Collectors.groupingBy(FlowGroup_Old_Compressed::getSrc_loc));

        // just post the update to RPCClient, not waiting for reply
        for (Map.Entry<String, List<FlowGroup_Old_Compressed>> entry : fgoBySA.entrySet()) {
            String saID = entry.getKey();
            List<FlowGroup_Old_Compressed> fgforSA = entry.getValue();

            // call async RPC
            rpcClientHashMap.get(saID).setFlow(fgforSA, netGraph, saID);
        }

/*        // How to parallelize -> use the threadpool
        List<FlowUpdateSender> tasks= new ArrayList<>();
        for ( Map.Entry<String,List<FlowGroup_Old_Compressed>> entry : fgoBySA.entrySet() ){
            tasks.add( new FlowUpdateSender(entry.getKey() , entry.getValue() , netGraph ) );
        }

        try {
            // wait for all sending to finish before proceeding
            List<Future<Integer>> futures = saControlExec.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
    }

    public void printMasterState() {
        StringBuilder str = new StringBuilder("-----Master state-----\n");
        int paused = 0;
        int running = 0;
        for (Map.Entry<String, Coflow> cfe : masterSharedData.coflowPool.entrySet()) {
            Coflow cf = cfe.getValue();

            str.append(cf.getId()).append('\n');

            for (Map.Entry<String, FlowGroup> fge : cf.getFlowGroups().entrySet()) {
                FlowGroup fg = fge.getValue();
                str.append(' ').append(fge.getKey()).append(' ').append(fg.getFlowGroupState())
                        .append(' ').append(fg.getTransmitted()).append(' ').append(fg.getTotalVolume()).append('\n');
                if (fg.getFlowGroupState() == FlowGroup.FlowGroupState.PAUSED) {
                    paused++;
                } else if (fg.getFlowGroupState() == FlowGroup.FlowGroupState.RUNNING) {
                    running++;
                }
            }
        }

        str.append("stats s: ").append(masterSharedData.flowStartCnt).append(" f: ").append(masterSharedData.flowFINCnt)
                .append(" p: ").append(paused).append(" r: ").append(running).append('\n');
        logger.info(str);
    }

    /**
     * Broadcast FlowInfo to corresponding SA/FA.
     * @param cf
     */
    public void broadcastFlowInfo(Coflow cf) {
        // TODO call the rpc, and set the FlowInfos


    }


/*    void testSA (String saIP, int saPort){

        final ManagedChannel channel = ManagedChannelBuilder.forAddress(saIP, saPort).usePlaintext(true).build();
        SAServiceGrpc.SAServiceBlockingStub blockingStub = SAServiceGrpc.newBlockingStub(channel);
        GaiaMessageProtos.PAM_REQ req = GaiaMessageProtos.PAM_REQ.newBuilder().build();

        Iterator<GaiaMessageProtos.PAMessage> it = blockingStub.prepareConnections(req);
        while (it.hasNext()) {
            System.out.print(it.next());
        }
        channel.shutdown();
    }*/
}
