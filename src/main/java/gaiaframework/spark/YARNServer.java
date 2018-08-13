package gaiaframework.spark;

import edu.umich.gaialib.GaiaAbstractServer;
import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.gaiamaster.Coflow;
import gaiaframework.gaiamaster.FlowGroup;
import gaiaframework.gaiamaster.Master;
import gaiaframework.gaiamaster.MasterSharedData;
import gaiaframework.util.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

public class YARNServer extends GaiaAbstractServer {

    private static final Logger logger = LogManager.getLogger();
    private final boolean isDebugMode;
    MasterSharedData msData;
    Configuration configuration;
    BufferedWriter bwrt;
    Master ms;

    public YARNServer(Configuration config, int port, MasterSharedData masterSharedData, boolean isDebugMode, Master ms) {
        super(port);
        this.msData = masterSharedData;
        this.configuration = config;
        this.isDebugMode = isDebugMode;
        this.ms = ms;

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
        Map<String, Map<String, List<ShuffleInfo.FlowInfo>>> groupedFlowInfo = pruneAndGroupFlowInfos(flowsList);

        if (flowsList.size() > 0) {
            Coflow cf = generateCoflow(cfID, groupedFlowInfo);
            long cfGenTime = System.currentTimeMillis();
            logger.info("YARN Server generated CF: {}, took {} ms", cf.getId(), (cfGenTime - cfStartTime));

            try {
                // we first broadcast flowInfos
                ms.broadcastFlowInfo(cf); // blocking!!
                long cfBCTime = System.currentTimeMillis();
                logger.info("Broadcast FlowInfo for CF: {}, took {} ms", cf.getId(), (cfBCTime - cfGenTime));
                // submit coflow to scheduler, no need to broadcast flowInfos, only broadcast the first time we schedule
                msData.onSubmitCoflow(cfID, cf);

                logger.info("Coflow {} submitted, total vol: {}", cf.getId(), (long) cf.getTotalVolume());
                bwrt.write("Coflow " + cf.getId() + " submitted, total vol: " + (long) cf.getTotalVolume() + "\n");
                bwrt.flush();

                cf.blockTillFinish();
                msData.onCoflowTransmissionFinish(cfID);

                long cfEndTime = System.currentTimeMillis();
                logger.info("Coflow {} finished in {} ms, returning to YARN", cfID, (cfEndTime - cfStartTime));
                bwrt.write("Coflow " + cf.getId() + " finished in (ms) " + (cfEndTime - cfStartTime) + "\n");
                bwrt.flush();
            } catch (InterruptedException e) {
                logger.error("ERROR occurred while submitting coflow");
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            logger.error("FATAL: CF {} is empty, skipping and returning to YARN", cfID);
            return;
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
     * Prune a list of flowInfo of {co-located, co-sited, index files, zero-volumed}. And group them by {srcLoc, dstLoc}
     *
     * @param flowList
     * @return groupedFlowInfo
     */
    private Map<String, Map<String, List<ShuffleInfo.FlowInfo>>> pruneAndGroupFlowInfos(List<ShuffleInfo.FlowInfo> flowList) {

        int flowCounter = 0;
        int beforePrune = flowList.size();

        // iterate through the list and prune
        Iterator<ShuffleInfo.FlowInfo> iter = flowList.iterator();
        Map<String, Map<String, List<ShuffleInfo.FlowInfo>>> groupedFlowInfo = new HashMap<>();

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
                continue;
            }

            // Filter same site
            assert srcLoc != null;
            if (srcLoc.equals(dstLoc)) {
                logger.warn("Ignoring Co-sited {}:{} {}", srcIP, dstIP, flowInfo.getDataFilename());
                iter.remove();
                continue;
            }

            // Filter volume < 1 flow
            long flowVolume = flowInfo.getFlowSize();
            if (flowVolume <= 0) {
                logger.warn("Ignoring size={} flow {}:{} {} ", flowVolume, srcIP, dstIP, flowInfo.getDataFilename());
                iter.remove();
                continue;
            }

            // Filter index files
            if (flowInfo.getDataFilename().endsWith("index")) {
                logger.warn("Ignoring index files {}:{} {}", srcIP, dstIP, flowInfo.getDataFilename());
                iter.remove();
                continue;
            }

            // Group the flowInfos
            if (groupedFlowInfo.containsKey(srcLoc)) {
                if (groupedFlowInfo.get(srcLoc).containsKey(dstLoc)) {
                    groupedFlowInfo.get(srcLoc).get(dstLoc).add(flowInfo);
                } else {
                    LinkedList<ShuffleInfo.FlowInfo> tmpList = new LinkedList<>();
                    tmpList.add(flowInfo);
                    groupedFlowInfo.get(srcLoc).put(dstLoc, tmpList);
                    flowCounter++;
                }
            } else {
                HashMap<String, List<ShuffleInfo.FlowInfo>> tmpMap = new HashMap<String, List<ShuffleInfo.FlowInfo>>();
                LinkedList<ShuffleInfo.FlowInfo> tmpList = new LinkedList<>();
                tmpList.add(flowInfo);
                tmpMap.put(dstLoc, tmpList);
                groupedFlowInfo.put(srcLoc, tmpMap);
                flowCounter++;
            }
        }

        logger.info("Prune {} flowInfos to {}", beforePrune, flowCounter);
        return groupedFlowInfo;
    }

    /**
     * Generate Coflow from a List of ShuffleInfo.FlowInfo
     *
     * @param cfID
     * @param groupedFlowInfos
     * @return
     */
    private Coflow generateCoflow(String cfID, Map<String, Map<String, List<ShuffleInfo.FlowInfo>>> groupedFlowInfos) {

        HashMap<String, FlowGroup> fgMap = new HashMap<>();

        for (Map.Entry<String, Map<String, List<ShuffleInfo.FlowInfo>>> srcEntry : groupedFlowInfos.entrySet()) {
            String srcLoc = srcEntry.getKey();
            for (Map.Entry<String, List<ShuffleInfo.FlowInfo>> dstEntry : srcEntry.getValue().entrySet()) {
                String dstLoc = dstEntry.getKey();
                List<ShuffleInfo.FlowInfo> flowInfos = dstEntry.getValue();

                // Create FG after extracted info, and add to coflow
                FlowGroup flowGroup = new FlowGroup(cfID, srcLoc, dstLoc, flowInfos);
                fgMap.put(flowGroup.getId(), flowGroup);
            }
        }

        return new Coflow(cfID, fgMap);
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
