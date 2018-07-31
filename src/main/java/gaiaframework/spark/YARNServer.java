package gaiaframework.spark;

import edu.umich.gaialib.GaiaAbstractServer;
import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.gaiamaster.Coflow;
import gaiaframework.gaiamaster.FlowGroup;
import gaiaframework.util.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class YARNServer extends GaiaAbstractServer {

    private static final Logger logger = LogManager.getLogger();
    private final boolean isDebugMode;
    LinkedBlockingQueue<Coflow> cfQueue;
    Configuration configuration;
    BufferedWriter bwrt;

    public YARNServer(Configuration config, int port, LinkedBlockingQueue<Coflow> coflowQueue, boolean isDebugMode) {
        super(port);
        this.cfQueue = coflowQueue;
        this.configuration = config;
        this.isDebugMode = isDebugMode;

        try {
            bwrt = new BufferedWriter(new java.io.FileWriter("/tmp/terra.txt"));
            bwrt.write("------- Server start at " + java.time.LocalDateTime.now() + " --------\n");
            bwrt.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * A blocking method to generate and process Coflow requests, block until Coflow finish.
     *
     * @param username
     * @param jobID
     * @param flowsList
     */
    @Override
    public void processReq(String username, String jobID, List<ShuffleInfo.FlowInfo> flowsList) {

        long cfStartTime = System.currentTimeMillis();

        // Create the CF and submit it.
        String cfID = username + "_" + jobID;

        logger.info("Pruning req: {} \n{}", cfID, flowsList.toArray());
        pruneFlowInfos(flowsList);

/*        if (flowsList.size() > 0) {
            Coflow cf = generateCoflow(cfID, flowsList);
            //TODO
        }*/


        // Aggregate all the flows by their Data Center location
        // Gaia only sees Data Centers
        // How to deal with co-located flows?

        HashMap<String, FlowGroup> coSiteFGs = new HashMap<>();
        HashMap<String, FlowGroup> flowGroups = new HashMap<>();
        HashMap<String, FlowGroup> indexFiles = new HashMap<>();

        generateFlowGroups_noAgg(cfID, flowsList, coSiteFGs, flowGroups, indexFiles);

        logger.error("{} co-located FG received by Gaia", coSiteFGs.size());


        try {

            if (flowGroups.size() == 0) {
                logger.error("FATAL: CF {} is empty, skipping and returning to YARN", cfID);
                // TODO Check this in the future, should not happen.
//                SCPTransferFiles_Serial(indexFiles);
//            SCPTransferFiles(indexFiles);
                return;
            } else {

                if (indexFiles.size() != 0) {
                    logger.warn("Received some index file, ignoring");
                }

                Coflow cf = new Coflow(cfID, flowGroups); // TODO change this v2.0

                logger.info("YARN Server submitting CF: {}", cf.getId());

                cfQueue.put(cf);
                logger.info("Coflow {} submitted, total vol: {}", cf.getId(), (long) cf.getTotalVolume());
                bwrt.write("Coflow " + cf.getId() + " submitted, total vol: " + (long) cf.getTotalVolume() + "\n");
                bwrt.flush();

                cf.blockTillFinish();

                long cfEndTime = System.currentTimeMillis();
                logger.info("Coflow {} finished in {} ms, returning to YARN", cfID, (cfEndTime - cfStartTime));
                bwrt.write("Coflow " + cf.getId() + " finished in (ms) " + (cfEndTime - cfStartTime) + "\n");
                bwrt.flush();
            }

        } catch (InterruptedException e) {
            logger.error("ERROR occurred while submitting coflow");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        if (isDebugMode) {
            System.out.println("Finished Shuffle, continue?");
            try {
                System.in.read();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Prune a list of flowInfo of {co-located, co-sited, index files, zero-volumed}
     * @param flowList
     */
    private void pruneFlowInfos(List<ShuffleInfo.FlowInfo> flowList) {

        // iterate through the list and prune
        Iterator<ShuffleInfo.FlowInfo> iter = flowList.iterator();

        while (iter.hasNext()) {
            ShuffleInfo.FlowInfo flowInfo = iter.next();

//            String srcIP = hardCodedURLResolver(flowInfo.getMapperIP());
//            String dstIP = hardCodedURLResolver(flowInfo.getReducerIP());
            String srcIP = (flowInfo.getMapperIP());
            String dstIP = (flowInfo.getReducerIP());

            String srcLoc = getTaskLocationIDfromIP(srcIP);
            String dstLoc = getTaskLocationIDfromIP(dstIP);

            // Filter same host
            if (srcIP.equals(dstIP)) {
                logger.warn("Ignoring Co-located {}:{} {}", srcIP, dstIP, flowInfo.getDataFilename());
                iter.remove();
            }

            // Filter same site
            assert srcLoc != null;
            if (srcLoc.equals(dstLoc)) {
                logger.warn("Ignoring Co-sited {}:{} {}", srcIP, dstIP, flowInfo.getDataFilename());
                iter.remove();
            }

            // Filter volume < 1 flow
            long flowVolume = flowInfo.getFlowSize();
            if (flowVolume <= 0) {
                logger.warn("Ignoring size={} flow {}:{} {} ", flowVolume, srcIP, dstIP, flowInfo.getDataFilename());
                iter.remove();
            }

            // Filter index files
            if (flowInfo.getDataFilename().endsWith("index")) {
                logger.warn("Ignoring index files {}:{} {}", srcIP, dstIP, flowInfo.getDataFilename());
                iter.remove();
            }
        }
    }

    /**
     * Generate Coflow from a List of ShuffleInfo.FlowInfo
     *
     * @param cfID
     * @param flowList
     * @return
     */
    private Coflow generateCoflow(String cfID, List<ShuffleInfo.FlowInfo> flowList) {

        // TODO create FGs from FlowInfos.
        return null;
    }


    // generate aggFlowGroups from req using an IP to ID mapping
    // this is the version without aggregation
    // Location encoding starts from 0
    // id - job_id:srcStage:dstStage:srcLoc-dstLoc // encoding task location info.
    // src - srcLoc
    // dst - dstLoc
    // owningCoflowID - dstStage
    // Volume - divided_data_size
    @Deprecated
    private HashMap<String, FlowGroup> generateFlowGroups_noAgg(String cfID, List<ShuffleInfo.FlowInfo> flowList, HashMap<String, FlowGroup> coSiteFGs,
                                                                HashMap<String, FlowGroup> outputFlowGroups, HashMap<String, FlowGroup> indexFileFGs) {

        // first store all flows into aggFlowGroups, then move the co-located ones to coLocatedFGs

        // for each FlowInfo, first find the fgID etc.
        for (ShuffleInfo.FlowInfo flowInfo : flowList) {

            String mapID = flowInfo.getMapAttemptID();
            String redID = flowInfo.getReduceAttemptID();

//            String srcIP = hardCodedURLResolver(flowInfo.getMapperIP());
//            String dstIP = hardCodedURLResolver(flowInfo.getReducerIP());
            String srcIP = (flowInfo.getMapperIP());
            String dstIP = (flowInfo.getReducerIP());


            String srcLoc = getTaskLocationIDfromIP(srcIP);
            String dstLoc = getTaskLocationIDfromIP(dstIP);

//            String srcLoc = getTaskLocationIDfromIP(srcIP);
//            String dstLoc = getTaskLocationIDfromIP(dstIP);

/*            String srcLoc = getTaskLocationID(mapID, req);
            String dstLoc = getTaskLocationID(redID, req);

            String srcIP = getRawAddrfromTaskID(mapID, req).split(":")[0];
            String dstIP = getRawAddrfromTaskID(redID, req).split(":")[0];*/


//            String afgID = cfID + ":" + srcLoc + '-' + dstLoc;
            String fgID = cfID + ":" + mapID + ":" + redID + ":" + srcLoc + '-' + dstLoc;

            // Filter same host
            if (srcIP.equals(dstIP)) {
                logger.warn("Ignoring Co-located {} {}", fgID, flowInfo.getDataFilename());
                continue;
            }

            // Gaia now uses bytes as the volume
            long flowVolume = flowInfo.getFlowSize();
            if (flowVolume == 0) {
                flowVolume = 1;
                // FIXME Terra now ignores flowVol = 0 flows
                logger.warn("Ignoring size=0 flow {} {} ", fgID, flowInfo.getDataFilename());
                continue;
            }
            FlowGroup fg = new FlowGroup(fgID, srcLoc, dstLoc, cfID, flowVolume,
                    flowInfo.getDataFilename(), mapID, redID);
            fg.flowInfos.add(flowInfo);
            fg.srcIPs.add(srcIP);
            fg.dstIPs.add(dstIP);


            // Filter index files
            if (flowInfo.getDataFilename().endsWith("index")) {
                logger.info("Got an index file {}", flowInfo.getDataFilename());
                indexFileFGs.put(fgID, fg);
            } else if (!srcLoc.equals(dstLoc)) { // Not co-sited FGs
                // Filter coSited flows
                outputFlowGroups.put(fgID, fg);
            } else { // co-sited FGs
                logger.warn("Got an co-sited flow {} {}", fgID, fg.getFilename());
                coSiteFGs.put(fgID, fg);
            }

        }

        // use this method to remove co-located
        for (String key : coSiteFGs.keySet()) {
            outputFlowGroups.remove(key);
            logger.warn("Removing co-sited FG {}", key);
        }

        return outputFlowGroups;
    }


    // TODO need to change this mechanism in the future // if same mapID and same dstLoc -> redundant
    private HashMap<String, FlowGroup> removeRedundantFlowGroups(HashMap<String, FlowGroup> inFlowGroups) {
        HashMap<String, FlowGroup> ret = new HashMap<>();

        for (Map.Entry<String, FlowGroup> fe : inFlowGroups.entrySet()) {

            boolean reduandant = false;
            for (Map.Entry<String, FlowGroup> rete : ret.entrySet()) {
                if (rete.getValue().getDstLocation().equals(fe.getValue().getDstLocation()) &&
                        rete.getValue().getMapID().equals(fe.getValue().getMapID())) {
                    reduandant = true;
                }
            }
            // check if this fe needs to be put in ret

            if (!reduandant) {
                ret.put(fe.getKey(), fe.getValue());
            }
        }

        return ret;
    }

    // 1. find the IP for this task using ShuffleInfo (first look in MapIP, then in ReduceIP)
    // 2. find the DCID for this IP?
/*    private String getTaskLocationID(String taskID, ShuffleInfo req) {
        String addr = getRawAddrfromTaskID(taskID, req);

        if (addr != null) {
            return configuration.findDCIDbyHostAddr(addr);
        }

        logger.error("Task IP not found for {}", taskID);
        return null;
    }*/

    private String getTaskLocationIDfromIP(String IP) {

        if (IP != null) {
            return configuration.findDCIDbyHostAddr(IP);
        }

        logger.error("Task IP is null");
        return null;
    }

/*    private String getRawAddrfromTaskID(String taskID, ShuffleInfo req) {
        // check the hostIP
        for (ShuffleInfo.MapperInfo mapperInfo : req.getMappersList()) {
            if (taskID.equals(mapperInfo.getMapperID())) {

                String addr_raw = mapperInfo.getMapperIP();


//                return addr_raw;
                return hardCodedURLResolver(addr_raw);
            }
        }

        // then check the reducerID
        for (ShuffleInfo.ReducerInfo reducerInfo : req.getReducersList()) {
            if (taskID.equals(reducerInfo.getReducerID())) {

                String addr_raw = reducerInfo.getReducerIP();

//                return addr_raw;
                return hardCodedURLResolver(addr_raw);
            }
        }

        return null;
    }*/


}
