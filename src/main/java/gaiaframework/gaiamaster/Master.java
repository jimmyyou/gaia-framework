package gaiaframework.gaiamaster;

import gaiaframework.comm.PortAnnouncementMessage_Old;
import gaiaframework.comm.PortAnnouncementRelayMessage;
import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.network.NetGraph;
import gaiaframework.network.Pathway;
import gaiaframework.scheduler.CoflowScheduler;
import gaiaframework.scheduler.ScheduleOutputFG;
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
//    protected Thread coflowListener;
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
            MasterRPCClient client = new MasterRPCClient(config.getSARPCHostname(id), config.getSAPort(id));
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
            // TODO(future) test the rules first before proceeding
            Thread.sleep(5000); // sleep 10s for the rules to propagate
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // start sending heartbeat
        logger.info("start heartbeat");
        for (String sa_id : netGraph.nodes_) {
            rpcClientHashMap.get(sa_id).startExp();
        }

        logger.info("All SA heartbeat started");
        logger.info("Master: starting periodical scheduler at every {} ms.", Constants.SCHEDULE_INTERVAL_MS);

        // start the periodic execution of schedule()
//        final Runnable runSchedule = () -> schedule();
        final Runnable runSchedule = () -> onSchedule();
        ScheduledFuture<?> mainHandler = mainExec.scheduleAtFixedRate(runSchedule, 0, Constants.SCHEDULE_INTERVAL_MS, MILLISECONDS);

        // Start the input
        yarnServer = new YARNServer(config, Constants.DEFAULT_YARN_PORT, masterSharedData, isDebugMode, this);
        try {
            yarnServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Master init finished, block the main thread (version 2.0)");
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

    /**
     * Called upon schedule timer tick (default 500ms).
     * Reads from the current coflow status and schedule.
     * we still need to take snapshot every 500ms. Because we need to ensure that the remaining volume is accurate.
     * Otherwise, the CF that are scheduled later may see a smaller volume hence are prioritized.
     */

    private void onSchedule() {
//        logger.info("schedule_New(): CF_ADD: {} CF_FIN: {} FG_FIN: {}", masterSharedData.flag_CF_ADD, masterSharedData.flag_CF_FIN, masterSharedData.flag_FG_FIN);

        long currentTime = System.currentTimeMillis();
        HashMap<String, ScheduleOutputFG> scheduledFGOs = new HashMap<>();
//        List<FlowGroup_Old_Compressed> decompressedFGOsToSend = new ArrayList<>();

        // snapshot??? We only need to snapshot when: 1. initCF 2. statusChange, so no need to snapshot here in v2.0

//        HashMap<String, Coflow_Old_Compressed> outcf = new HashMap<>();
//        for (Map.Entry<String, Coflow> ecf : masterSharedData.coflowPool.entrySet()) {
//            Coflow_Old_Compressed cfo = Coflow.toCoflow_Old_Compressed_with_Trimming(ecf.getValue());
//            outcf.put(cfo.getId(), cfo);
//        }

        // process Link change
        GaiaMessageProtos.PathStatusReport m = masterSharedData.linkStatusQueue.poll();
        while (m != null) {
            scheduler.processLinkChange(m);
            m = masterSharedData.linkStatusQueue.poll();

            // Fake a CF_ADD to trigger reschedule here
            masterSharedData.flag_CF_ADD = true;
        }

//        printCFList(outcf);

        // Process the events and update the scheduler state.
        if (masterSharedData.flag_CF_ADD) { // redo sorting, may result in preemption
            masterSharedData.flag_CF_ADD = false;
            masterSharedData.flag_CF_FIN = false;
            masterSharedData.flag_FG_FIN = false;

            // update the CF_Status in scheduler
            scheduler.resetCFList(masterSharedData.coflowPool);

//            scheduler.printCFList();

        } else if (masterSharedData.flag_CF_FIN) { // no LP-sort, just update volume status and re-schedule
            masterSharedData.flag_CF_FIN = false;
            masterSharedData.flag_FG_FIN = false;

            scheduler.updateCFList(masterSharedData.coflowPool);
//            scheduler.handleCoflowFIN(outcf);

//            scheduler.printCFList();

        } else if (masterSharedData.flag_FG_FIN) { // no-reschedule, just pick up a new flowgroup.
            masterSharedData.flag_FG_FIN = false;

            scheduler.updateCFList(masterSharedData.coflowPool);
//            scheduler.handleFlowGroupFIN(outcf);

//            scheduler.printCFList();

        } else {  // if all flags are false, NOP
            return; // no need to print out the execution time.
        }

        // Schedule and send CTRL Msg.
        try {
            scheduledFGOs = scheduler.scheduleRRF(currentTime);

//            for (Map.Entry<String, ScheduleOutputFG> fgoE : scheduledFGOs.entrySet()) {
//                ScheduleOutputFG fgo = fgoE.getValue();
//                logger.info("FGOID: {}, {} / {} : {}", fgoE.getKey(), fgo.getId(), fgo.getCoflow_id(), fgo.getFgoState());
//            }

            // generate and send rpc msgs. 1. parse FGState 2. gen msg 3. send msg
            generateAndSendCtrlMsg(scheduledFGOs);
//                decompressedFGOsToSend = parseFlowState_DeCompress(masterSharedData, scheduledFGOs);
//                sendControlMessages_Async(decompressedFGOsToSend);

        } catch (Exception e) {
            e.printStackTrace();
        }

        long deltaTime = System.currentTimeMillis() - currentTime;

        StringBuilder fgoContent = new StringBuilder("\n");
        for (Map.Entry<String, ScheduleOutputFG> fgoe : scheduledFGOs.entrySet()) {
            ScheduleOutputFG fgo = fgoe.getValue();
            fgoContent.append(fgo.getId()).append(' ').append(fgo.getPaths()).append(' ').append(fgo.getFgoState()).append('\n');
        }
        logger.info("FG content: {}", fgoContent);
        logger.info("schedule(): took {} ms. Active CF: {} Scheduled FG: {}", deltaTime, masterSharedData.coflowPool.size(), scheduledFGOs.size());

//        printMasterState();
    }

    /**
     * Generate Ctrl Msg from scheduled output, according to states in sharedData.cfPool
     *
     * @param scheduledFGOs
     */
    // TODO(future) when rates don't change, dont send them. i.e. speculatively send change message
    private void generateAndSendCtrlMsg(HashMap<String, ScheduleOutputFG> scheduledFGOs) {

        // traverse all FGs in CFPool, and modify the scheduledFGOs
        for (Map.Entry<String, Coflow> ecf : masterSharedData.coflowPool.entrySet()) {
            Coflow cf = ecf.getValue();
            // Traversing all FGs in CFPool
            for (Map.Entry<String, FlowGroup> fge : cf.getFlowGroups().entrySet()) {
                FlowGroup fg = fge.getValue();
                String fgID = fge.getKey();
                if (fg.getFlowGroupState() == FlowGroup.FlowGroupState.TRANSFER_FIN) {
                    // TODO(future) may repeat too many times here. (Because FGs are skewed.)
                    logger.info("find fg {} in TRANSFER_FIN state, to be removed from scheduledFGOs", fgID);
                    scheduledFGOs.remove(fgID); // to be removed from scheduledFGOs
                    continue; // ignore finished, they shall be removed shortly
                } else if (fg.getFlowGroupState() == FlowGroup.FlowGroupState.RUNNING) { // may pause/change the running flow
                    if (scheduledFGOs.containsKey(fgID)) { // we may need to change, if the path/rate are different
                        scheduledFGOs.get(fgID).setFgoState(ScheduleOutputFG.FGOState.CHANGING);
                        fg.setTotalRate(scheduledFGOs.get(fgID).getTotalRate());
                        // TODO we can check here to see if we indeed need to send out the updated rate.
//                        fgoToSend.add(fgoHashMap.get(fg.getId()).setFlowState(FlowGroup_Old_Compressed.FlowState.CHANGING)); // running flow needs to change
                    } else { // we need to pause, because there is no scheduled FG in scheduledFGOs
                        fg.setFlowGroupState(FlowGroup.FlowGroupState.PAUSED);
                        fg.setTotalRate(0);
                        // Here we need to create a fake scheduledFGO to send the PAUSE msg.
                        ScheduleOutputFG tmpFGO = new ScheduleOutputFG(fgID, fg.getSrcLocation(), fg.getDstLocation(),
                                ScheduleOutputFG.FGOState.PAUSING, fg.getOwningCoflowID());
                        scheduledFGOs.put(fgID, tmpFGO);
//                        fgoToSend.add(fg.toFlowGroup_Old(0).setFlowState(FlowGroup_Old_Compressed.FlowState.PAUSING));
                        //                        fgoToSend.add ( FlowGroup.toFlowGroup_Old(fg, 0).setFlowGroupState(FlowGroup_Old_Compressed.FlowGroupState.PAUSING) );
                    }
                } else { // case: NEW/PAUSED
//                    logger.info("DEBUG: FG {} not in RUNNING state", fgID);
                    if (scheduledFGOs.containsKey(fgID)) { // we take action only if the flow get (re)scheduled
                        if (fg.getFlowGroupState() == FlowGroup.FlowGroupState.NEW) { // start the flow
                            fg.setFlowGroupState(FlowGroup.FlowGroupState.RUNNING);
                            fg.setTotalRate(scheduledFGOs.get(fgID).getTotalRate());
                            scheduledFGOs.get(fgID).setFgoState(ScheduleOutputFG.FGOState.STARTING);
//                            logger.info("DEBUG: set fgo {} state to {}", fgID, scheduledFGOs.get(fgID).getFgoState());
//                            fgoToSend.add(fgoHashMap.get(fg.getId()).setFlowState(FlowGroup_Old_Compressed.FlowState.STARTING));
                            masterSharedData.flowStartCnt++; // Simply for stats
                        } else if (fg.getFlowGroupState() == FlowGroup.FlowGroupState.PAUSED) { // RESUME the flow
                            fg.setFlowGroupState(FlowGroup.FlowGroupState.RUNNING);
                            fg.setTotalRate(scheduledFGOs.get(fgID).getTotalRate());
                            scheduledFGOs.get(fgID).setFgoState(ScheduleOutputFG.FGOState.CHANGING);
//                            fgoToSend.add(fgoHashMap.get(fgID).setFlowState(FlowGroup_Old_Compressed.FlowState.CHANGING));
                        }
                    }
                }
            }
        }

        // send CTRL msg
        sendControlMessages_Async(scheduledFGOs);

    }

    /**
     * Asynchronously send CTRL Msg.
     *
     * @param scheduledFGOs
     */
    private void sendControlMessages_Async(HashMap<String, ScheduleOutputFG> scheduledFGOs) {

        // First, group msgs by sending agent.
        Map<String, List<ScheduleOutputFG>> fgobySA = scheduledFGOs.entrySet().stream().map(Map.Entry::getValue)
                .collect(Collectors.groupingBy(ScheduleOutputFG::getSrc_loc));

        for (Map.Entry<String, List<ScheduleOutputFG>> entry : fgobySA.entrySet()) {
            String saID = entry.getKey();

            // call async rpc
            rpcClientHashMap.get(saID).setFlowNew(entry.getValue(), netGraph, saID, masterSharedData);
        }
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
     * Broadcast FlowInfo to corresponding Agents.
     *
     * @param cf
     */
    public void broadcastFlowInfo(Coflow cf) throws InterruptedException {
        // HOWTO: iterate through all FGs, and send FGIBundle message.
        // For now only BC to the receiving side, BC to the sending side will be performed at START msg

        // First sort the FGs by FA.
        Map<String, List<FlowGroup>> fgByFA = cf.getFlowGroups().entrySet().stream().map(Map.Entry::getValue)
                .collect(Collectors.groupingBy(FlowGroup::getDstLocation));

        // Call rpc, and count down for all complete msgs.
        CountDownLatch latch = new CountDownLatch(fgByFA.size());

        for (Map.Entry<String, List<FlowGroup>> entry : fgByFA.entrySet()) {
            String saID = entry.getKey();

            // call async RPC
            rpcClientHashMap.get(saID).SetFlowInfoList(entry.getValue(), latch);
        }

        // wait for broadcast to complete
        latch.await();
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
