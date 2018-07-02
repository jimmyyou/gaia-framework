package gaiaframework.spark;

import edu.umich.gaialib.GaiaAbstractServer;
import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.gaiamaster.Coflow;
import gaiaframework.gaiamaster.FlowGroup;
import gaiaframework.util.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class YARNServer extends GaiaAbstractServer {

    private static final Logger logger = LogManager.getLogger();
    LinkedBlockingQueue<Coflow> cfQueue;
    Configuration configuration;

    public YARNServer(Configuration config, int port, LinkedBlockingQueue<Coflow> coflowQueue) {
        super(port);
        this.cfQueue = coflowQueue;
        this.configuration = config;
    }

    @Override
    public void processReq(ShuffleInfo req) {
        logger.info("received shuffle info: {}", req);

        // Create the CF and submit it.
        String cfID = req.getUsername() + ":" + req.getJobID();

        // Aggregate all the flows by their Data Center location
        // Gaia only sees Data Centers
        // How to deal with co-located flows?

        HashMap<String, FlowGroup> coSiteFGs = new HashMap<>();
        HashMap<String, FlowGroup> flowGroups = new HashMap<>();
        HashMap<String, FlowGroup> indexFiles = new HashMap<>();

        generateFlowGroups_noAgg(cfID, req, coSiteFGs, flowGroups, indexFiles);
        Coflow cf = new Coflow(cfID, flowGroups);

        logger.info("YARN Server submitting CF: {}", cf.toPrintableString());

        if (coSiteFGs.size() >= 0) {
            logger.error("{} co-located FG received by Gaia", coSiteFGs.size());
        }

        try {

            // Try using SCP to transfer index files for now.
            // FIXME SCP will NEVER scale!!!
//            SCPTransferFiles_Serial(indexFiles);
            SCPTransferFiles(indexFiles);

            if (cf.getFlowGroups().size() == 0) {
                logger.info("CF {} is empty, skipping", cf.getId());
                return;
            }

            cfQueue.put(cf);
            logger.info("Coflow submitted, Trapping into waiting for coflow to finish");


            cf.blockTillFinish();
//            ShuffleTask st = new ShuffleTask(cf);
//            st.run(); // wait for it to finish
        } catch (InterruptedException e) {
            logger.error("ERROR occurred while submitting coflow");
            e.printStackTrace();
        }

        // FIXME: sleep 1s to ensure that the file is fully written to disk
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void SCPTransferFiles(HashMap<String, FlowGroup> fgsToSCP) {

        // Create a list of cmds;
        List<String> cmds = new ArrayList<>();
        for (FlowGroup fg : fgsToSCP.values()) {

            String filepath = fg.getFilename();

            if (fg.dstIPs.size() > 1) {
                logger.error("indexFiles FG have multiple dstIP!");
            }

            String trimmedDirPath = filepath.substring(0, filepath.lastIndexOf("/"));
            String cmd_mkdir = "ssh jimmyyou@" + fg.dstIPs.get(0) + " mkdir -p " + trimmedDirPath;
            String cmd_scp = "scp " + fg.srcIPs.get(0) + ":" + filepath + " " + fg.dstIPs.get(0) + ":" + filepath;
            String cmd = cmd_mkdir + " ; " + cmd_scp;

            cmds.add(cmd);

        }
        // Then remove duplicate commands
        List<String> dedupedCmds = cmds.stream().distinct().collect(Collectors.toList());
        logger.info("Trimmed {} SCP commands", (cmds.size() - dedupedCmds.size()));

        List<Process> pool = new ArrayList<>();
        for (String cmd : cmds) {
            Process p = null;
            try {

                p = Runtime.getRuntime().exec(cmd);
                pool.add(p);
                logger.info("Exec (submitted): {}", cmd);

/*                String line;
                BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
                while ((line = bri.readLine()) != null) {
                    System.out.println(line);
                }

                bri = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                while ((line = bri.readLine()) != null) {
                    System.out.println(line);
                }*/
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Wait for all cmd to finish
        logger.info("Waiting for SCP file transfer");
        for (Process p : pool) {
            try {
                p.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("SCP file transfer finished");
    }

    private void SCPTransferFiles_Serial(HashMap<String, FlowGroup> fgsToSCP) throws InterruptedException {

        // Create a list of cmds;
        List<String> cmds = new ArrayList<>();
        for (FlowGroup fg : fgsToSCP.values()) {

            String filepath = fg.getFilename();

            if (fg.dstIPs.size() > 1) {
                logger.error("indexFiles FG have multiple dstIP!");
            }

            String trimmedDirPath = filepath.substring(0, filepath.lastIndexOf("/"));
            String cmd_mkdir = "ssh jimmyyou@" + fg.dstIPs.get(0) + " mkdir -p " + trimmedDirPath;
            String cmd_scp = "scp " + fg.srcIPs.get(0) + ":" + filepath + " " + fg.dstIPs.get(0) + ":" + filepath;
            String cmd = cmd_mkdir + " ; " + cmd_scp;

            cmds.add(cmd);

        }

        // Then remove duplicate commands
        List<String> dedupedCmds = cmds.stream().distinct().collect(Collectors.toList());
        logger.info("Trimmed {} SCP commands", (cmds.size() - dedupedCmds.size()));

        for (String cmd : dedupedCmds) {
            logger.info("Invoking {}", cmd);

            Process p = null;
            try {
                p = Runtime.getRuntime().exec(cmd);
                p.waitFor();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        logger.info("SCP file transfer finished");

        /*for (String cmd : cmds) {
            Process p = null;
            try {

                p = Runtime.getRuntime().exec(cmd);
                pool.add(p);
                logger.info("Exec: {}", cmd);

*//*                String line;
                BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
                while ((line = bri.readLine()) != null) {
                    System.out.println(line);
                }

                bri = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                while ((line = bri.readLine()) != null) {
                    System.out.println(line);
                }*//*
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
*/


    }

    /*// generate aggFlowGroups from req using an IP to ID mapping
    // this is the version with aggregation
    // Location encoding starts from 0
    // id - job_id:srcStage:dstStage:srcLoc-dstLoc // encoding task location info.
    // src - srcLoc
    // dst - dstLoc
    // owningCoflowID - dstStage
    // Volume - divided_data_size
    private HashMap<String, FlowGroup> generateFlowGroups(String cfID, ShuffleInfo req, HashMap<String, FlowGroup> coLocatedFGs,
                                                          HashMap<String, FlowGroup> aggFlowGroups, HashMap<String, FlowGroup> indexFileFGs) {

        // first store all flows into aggFlowGroups, then move the co-located ones to coLocatedFGs

        // for each FlowInfo, first find the fgID etc.
        for (ShuffleInfo.FlowInfo flowInfo : req.getFlowsList()) {

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

*//*            String srcLoc = getTaskLocationID(mapID, req);
            String dstLoc = getTaskLocationID(redID, req);

            String srcIP = getRawAddrfromTaskID(mapID, req).split(":")[0];
            String dstIP = getRawAddrfromTaskID(redID, req).split(":")[0];*//*


            String afgID = cfID + ":" + srcLoc + '-' + dstLoc;
            String fgID = cfID + ":" + mapID + ":" + redID + ":" + srcLoc + '-' + dstLoc;

            // Filter same location
            if (srcLoc.equals(dstLoc)) continue;

            // check if we already have this fg.
            if (aggFlowGroups.containsKey(afgID)) {
                FlowGroup fg = aggFlowGroups.get(afgID);

                fg.flowInfos.add(flowInfo);
                fg.srcIPs.add(srcIP);
                fg.dstIPs.add(dstIP);
                fg.addTotalVolume(flowInfo.getFlowSize());

                logger.info("WARN: Add Flow {} to existing FG {}", flowInfo.getDataFilename(), afgID);

            } else {

                // Gaia now uses bytes as the volume
                long flowVolume = flowInfo.getFlowSize();
                if (flowVolume == 0) flowVolume = 1;
                FlowGroup fg = new FlowGroup(afgID, srcLoc, dstLoc, cfID, flowVolume,
                        flowInfo.getDataFilename(), mapID, redID);
                fg.flowInfos.add(flowInfo);
                fg.srcIPs.add(srcIP);
                fg.dstIPs.add(dstIP);
                aggFlowGroups.put(afgID, fg);

                // Filter out all co-located flows
                if (srcLoc.equals(dstLoc)) {
                    coLocatedFGs.put(fgID, fg);
                }

                // Filter index files
                if (flowInfo.getDataFilename().endsWith("index")) {
                    logger.info("Got an index file {}", flowInfo.getDataFilename());
                    indexFileFGs.put(fgID, fg);
                }


            }
        }

        for (String key : coLocatedFGs.keySet()) {
            aggFlowGroups.remove(key);
        }

        return aggFlowGroups;
    }
*/

    // generate aggFlowGroups from req using an IP to ID mapping
    // this is the version without aggregation
    // Location encoding starts from 0
    // id - job_id:srcStage:dstStage:srcLoc-dstLoc // encoding task location info.
    // src - srcLoc
    // dst - dstLoc
    // owningCoflowID - dstStage
    // Volume - divided_data_size
    private HashMap<String, FlowGroup> generateFlowGroups_noAgg(String cfID, ShuffleInfo req, HashMap<String, FlowGroup> coSiteFGs,
                                                                HashMap<String, FlowGroup> outputFlowGroups, HashMap<String, FlowGroup> indexFileFGs) {

        // first store all flows into aggFlowGroups, then move the co-located ones to coLocatedFGs

        // for each FlowInfo, first find the fgID etc.
        for (ShuffleInfo.FlowInfo flowInfo : req.getFlowsList()) {

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
                logger.warn("Got an co-sited flow");
                coSiteFGs.put(fgID, fg);
            }

        }

        // use this method to remove co-located
        for (String key : coSiteFGs.keySet()) {
            outputFlowGroups.remove(key);
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

    private String hardCodedURLResolver(String url) {
        // New version on "new3" experiment
        if (url.equals("clnode075.clemson.cloudlab.us:8042")) {
            return "10.0.1.1";
        }
        if (url.equals("clnode049.clemson.cloudlab.us:8042")) {
            return "10.0.1.2";
        }
        if (url.equals("clnode053.clemson.cloudlab.us:8042")) {
            return "10.0.1.3";
        }

        if (url.equals("clnode048.clemson.cloudlab.us:8042")) {
            return "10.0.2.1";
        }
        if (url.equals("clnode052.clemson.cloudlab.us:8042")) {
            return "10.0.2.2";
        }
        if (url.equals("clnode096.clemson.cloudlab.us:8042")) {
            return "10.0.2.3";
        }


        if (url.equals("clnode058.clemson.cloudlab.us:8042")) {
            return "10.0.3.1";
        }
        if (url.equals("clnode067.clemson.cloudlab.us:8042")) {
            return "10.0.3.2";
        }
        if (url.equals("clnode055.clemson.cloudlab.us:8042")) {
            return "10.0.3.3";
        }

/*        if(url.equals("clnode045.clemson.cloudlab.us:8042")){
            return "10.0.1.1";
        }

        if(url.equals("clnode072.clemson.cloudlab.us:8042")){
            return "10.0.1.2";
        }

        if(url.equals("clnode062.clemson.cloudlab.us:8042")){
            return "10.0.1.3";
        }

        if(url.equals("clnode093.clemson.cloudlab.us:8042")){
            return "10.0.2.3";
        }

        if(url.equals("clnode056.clemson.cloudlab.us:8042")){
            return "10.0.2.1";
        }

        if(url.equals("clnode088.clemson.cloudlab.us:8042")){
            return "10.0.2.2";
        }*/

        return url;
    }
}
