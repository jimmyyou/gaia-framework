package gaiaframework.scheduler;

import gaiaframework.gaiamaster.Coflow;
import gaiaframework.gaiamaster.FlowGroup;
import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.mmcf.MMCFOptimizer;
import gaiaframework.network.*;
import gaiaframework.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.graphstream.graph.Edge;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// coflow scheduler 0.2.0

public class CoflowScheduler extends Scheduler {

    private static final Logger logger = LogManager.getLogger();
    private final SubscribedLink[][] linksAtStart;

    protected Comparator<CoflowSchedulerEntry> smallCCTFirst = Comparator.comparingDouble(o -> o.getLastLPOutput().completion_time_);

    protected List<CoflowSchedulerEntry> cfseList = new LinkedList<>();
//    List<CoflowSchedulerEntry> nonDDLCFList = new LinkedList<>();

    // Create a priority queue to sort the CFs according to completion time.
//    protected Queue<CoflowSchedulerEntry> cfQueue = new PriorityQueue<>(smallCCTFirst);

    public CoflowScheduler(NetGraph net_graph) {
        super(net_graph);

        // init empty links
        linksAtStart = new SubscribedLink[net_graph_.nodes_.size()][net_graph_.nodes_.size()];
        for (Edge e : net_graph_.graph_.getEachEdge()) {
            int src = Integer.parseInt(e.getNode0().toString());
            int dst = Integer.parseInt(e.getNode1().toString());
            linksAtStart[src][dst] = new SubscribedLink(Double.parseDouble(e.getAttribute("bandwidth").toString()));
            linksAtStart[dst][src] = new SubscribedLink(Double.parseDouble(e.getAttribute("bandwidth").toString()));
        }

    }

    public void processLinkChange(GaiaMessageProtos.PathStatusReport m) {

        int pathID = m.getPathID();
        String saID = m.getSaID();
        String raID = m.getRaID();
        logger.warn("Link Change: {} {} {} isBroken: {}", saID, raID, pathID, m.getIsBroken());

        Pathway p = net_graph_.apap_.get(saID).get(raID).get(pathID);

        assert (p.node_list.size() == 2);
        int src = Integer.parseInt(saID);
        int dst = Integer.parseInt(raID);

        if (m.getIsBroken()) {

            linksAtStart[src][dst].goDown();
            links_[src][dst].goDown();
            logger.info("Making Link {} {} go down", src, dst);

        } else {

            linksAtStart[src][dst].goUp();
            links_[src][dst].goUp();
            logger.info("Making Link {} {} go up", src, dst);
        }
    }


    /**
     * A class to take snapshot only store information that are related to scheduling (src,dst,vol,int_id).
     */
    public class CoflowSchedulerEntry {

        public class FlowGroupSchedulerEntry {
            public String fgID;
            public String srcLoc;
            public String dstLoc;
            public String coflowID;
            public int intID;
            public double remainingVol;

            public FlowGroupSchedulerEntry(String fgID, String srcLoc, String dstLoc, int intID, double remainingVol, String owningCoflowID) {
                this.fgID = fgID;
                this.srcLoc = srcLoc;
                this.dstLoc = dstLoc;
                this.intID = intID;
                this.remainingVol = remainingVol;
                this.coflowID = owningCoflowID;
            }
        }

        String cfID;
        Coflow coflow;
        HashMap<String, FlowGroupSchedulerEntry> flowgroups;
        MMCFOptimizer.MMCFOutput lastLPOutput = null;

        public CoflowSchedulerEntry(Coflow cf) {

            this.cfID = cf.getId();
            this.flowgroups = new HashMap<>();
            this.coflow = cf;

            int intIDCount = 0;
            for (Map.Entry<String, FlowGroup> fge : cf.getFlowGroups().entrySet()) {
                FlowGroup fg = fge.getValue();

                // Snapshot here!
                FlowGroupSchedulerEntry fgse = new FlowGroupSchedulerEntry(fg.getId(), fg.getSrcLocation(), fg.getDstLocation(),
                        (intIDCount++), fg.getRemainingVolume(), fg.getOwningCoflowID());

                flowgroups.put(fg.getId(), fgse);
            }

        }

        /**
         * update the volume of FGs of this CF accordingly
         *
         * @param cf
         */
        public void updateVolume(Coflow cf) {

            // Iterate through flowgroups, remove the finished ones, copy the others.
            for (Map.Entry<String, FlowGroup> fge : cf.getFlowGroups().entrySet()) {
                FlowGroup fg = fge.getValue();
                String fgID = fge.getKey();
                if (fg.getRemainingVolume() < Constants.DOUBLE_EPSILON) {
                    this.flowgroups.remove(fgID);
                } else {
                    if (this.flowgroups.containsKey(fgID)) {
                        this.flowgroups.get(fgID).remainingVol = fg.getRemainingVolume();
                    } else {
                        logger.error("FATAL: No existing SchedulerFGE for {}", fgID);
                        throw new RuntimeException("FATAL: No existing SchedulerFGE");
                    }
                }
            }
        }

        public void setLastLPOutput(MMCFOptimizer.MMCFOutput lastLPOutput) {
            this.lastLPOutput = lastLPOutput;
        }

        public String getCfID() {
            return cfID;
        }

        public Coflow getCoflow() {
            return coflow;
        }

        public HashMap<String, FlowGroupSchedulerEntry> getFlowgroups() {
            return flowgroups;
        }

        public MMCFOptimizer.MMCFOutput getLastLPOutput() {
            return lastLPOutput;
        }
    }

/*    public class CoflowSchedulerEntry {
        Double cct;
        Coflow_Old_Compressed cf;
        MMCFOptimizer.MMCFOutput lastLPOutput;

        public void setLastLPOutput(MMCFOptimizer.MMCFOutput lastLPOutput) {
            this.lastLPOutput = lastLPOutput;
        }

        public CoflowSchedulerEntry(Coflow_Old_Compressed cf, MMCFOptimizer.MMCFOutput mmcfOutput) {
            this.cf = cf;
            this.lastLPOutput = mmcfOutput;
            this.cct = mmcfOutput.completion_time_;
        }

    }*/

/*    public boolean checkDDL(Coflow cf) {

        if (cf.ddl_Millis < 0) {
            return true;
        }

        reset_linksWithDDLCF();

        for (CoflowSchedulerEntry cfe : cfseList) {
            if (cfe.cf.ddl_Millis > 0) { // it has deadline

                for (Map.Entry<String, FlowGroup_Old_Compressed> fe : cfe.cf.flows.entrySet()) {
                    subscribeFlowToLinkWithDDFCF(fe.getValue());
                }
            }
        }

        Coflow_Old_Compressed cfo = Coflow.toCoflow_Old_Compressed_with_Trimming(cf);

        logger.info("Received DDL Coflow {}", cf.getId());
        // then check the ddl against the current "DDL ONLY" link status
        MMCFOptimizer.MMCFOutput mmcf_out = null; // This is the recursive part.
        try {
            mmcf_out = MMCFOptimizer.glpk_optimize(cfo, net_graph_, linksWithDDLCF);

            // we only check the ddl Once!
            if (mmcf_out.completion_time_ > 0 && mmcf_out.completion_time_ * 1000 <= cf.ddl_Millis) {
                logger.info("Admitting DDL Coflow {}", cf.getId());

                boolean all_flows_scheduled = true;
                for (String k : cfo.flows.keySet()) {
                    FlowGroup_Old_Compressed f = cfo.flows.get(k);
                    if (!f.isDone()) {
                        if (mmcf_out.flow_link_bw_map_.get(f.getInt_id()) == null) {
                            all_flows_scheduled = false;
                        }
                    }
                }

                if (!all_flows_scheduled) {
                    logger.error("Admitted a wrong DDL coflow {}", cf.getId());
                }

                return true;
            } else {
                logger.info("DDL Coflow {} rejected: deadline {} and CCT {}", cf.getId(), cf.ddl_Millis, mmcf_out.completion_time_);
                logger.error("DDL Coflow {} rejected: deadline {} and CCT {}", cf.getId(), cf.ddl_Millis, mmcf_out.completion_time_);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return false;
    }*/

//    public void finish_flow(FlowGroup_Old_Compressed f) {
//        for (Pathway p : f.paths) {
//            for (int i = 0; i < p.node_list.size() - 1; i++) {
//                int src = Integer.parseInt(p.node_list.get(i));
//                int dst = Integer.parseInt(p.node_list.get(i+1));
//                links_[src][dst].subscribers_.remove(p);
//            }
//        }
//    }

    public void make_paths(ScheduleOutputFG f, ArrayList<Link> link_vals) {
        // TODO(jack): Consider just choosing the shortest path (measured by hops)
        //       from src to dst if the flow has volume below some threshold.
        //       See if not accounting for bw consumption on a certain link
        //       makes any affect.

        // This portion is similar to FlowGroup::find_pathway_with_link_allocation in Sim
        LinkedList<Pathway> potential_paths = new LinkedList<Pathway>();
        LinkedList<Pathway> completed_paths = new LinkedList<Pathway>();

        // Find all links in the network from the flow's source that have some bandwidth
        // availible and start paths from them.
        ArrayList<Link> links_to_remove = new ArrayList<Link>();
        for (Link l : link_vals) {
            if (l.src_loc_.equals(f.getSrc_loc())) {
                Pathway p = new Pathway();
                p.node_list.add(l.src_loc_);
                p.node_list.add(l.dst_loc_);
//                p.bandwidth = l.cur_bw_;
                p.setBandwidth(l.cur_bw_);

                if (l.dst_loc_.equals(f.getDst_loc())) {
                    completed_paths.add(p);
                } else {
                    potential_paths.add(p);
                }

                links_to_remove.add(l);
            }
        }

        // Remove any Links that were added above
        for (Link l : links_to_remove) {
            link_vals.remove(l);
        }

        // Iterate through remaining links and try to add them to paths
        ArrayList<Pathway> paths_to_remove = new ArrayList<Pathway>();
        boolean link_added;
        while (!link_vals.isEmpty()) {
            links_to_remove.clear();
            link_added = false;

            for (Link l : link_vals) {
                if (l.cur_bw_ == 0.0) {
                    links_to_remove.add(l);
                    continue;
                }

                for (Pathway p : potential_paths) {
                    // Does this link fit after the current last node in the path?
                    if (!p.dst().equals(l.src_loc_)) {
                        continue;
                    }

                    // Does the bandwidth available on this link directly match the bandwidth
                    // of this pathway?
                    if (Math.round(Math.abs(p.getBandwidth() - l.cur_bw_) * 100.0) / 100.0 < 0.01) {
                        p.node_list.add(l.dst_loc_);
                        link_added = true;

                        // Check if path is now complete
                        if (l.dst_loc_.equals(f.getDst_loc())) {
                            paths_to_remove.add(p);
                            completed_paths.add(p);
                        }

                        links_to_remove.add(l);
                        break;
                    }

                    // Does this link have less bandwidth than the bandwidth available on the path?
                    // Split the path in two -- one path taking this link (and reducing its bandwidth)
                    // and the other not taking the path and using the remaining bandwidth.
                    else if (Math.round((p.getBandwidth() - l.cur_bw_) * 100.0) / 100.0 >= 0.01) {
                        Pathway new_p = new Pathway();
//                        new_p.bandwidth = p.getBandwidth() - l.cur_bw_;
                        new_p.setBandwidth(p.getBandwidth() - l.cur_bw_);
                        new_p.node_list = (ArrayList<String>) p.node_list.clone();
                        potential_paths.add(new_p);
//                        p.bandwidth = l.cur_bw_;
                        p.setBandwidth(l.cur_bw_);
                        p.node_list.add(l.dst_loc_);
                        link_added = true;

                        // Check if path is now complete
                        if (l.dst_loc_.equals(f.getDst_loc())) {
                            paths_to_remove.add(p);
                            completed_paths.add(p);
                        }

                        links_to_remove.add(l);
                        break;
                    }

                    // Does the link have more bandwidth than the bandwidth available on the path?
                    // Only reduce the link's bandwidth by the amount that could be used by the path.
                    else if (Math.round((p.getBandwidth() - l.cur_bw_) * 100.0) / 100.0 <= -0.01) {
                        l.cur_bw_ = l.cur_bw_ - p.getBandwidth();
                        p.node_list.add(l.dst_loc_);
                        link_added = true;
                        // Check if path is now complete
                        if (l.dst_loc_.equals(f.getDst_loc())) {
                            paths_to_remove.add(p);
                            completed_paths.add(p);
                        }
                        // TODO(jack): Consider breaking here -- old simulator does so...
                    }

                } // for pathway

                // Remove any paths that have been completed during this last round
                for (Pathway p : paths_to_remove) {
                    potential_paths.remove(p);
                }

            } // for link

            // Remove any Links that were added above
            for (Link l : links_to_remove) {
                link_vals.remove(l);
            }

            // If we were unable to add any links this round, just quit
            if (!link_added) {
                break;
            }
        } // while link_vals
        f.paths.clear();
        f.paths = completed_paths;
    }

//    public void progress_flow(FlowGroup_Old_Compressed f) {
//        for (Pathway p : f.paths) {
//            f.setTransmitted_volume(f.getTransmitted_volume() + p.getBandwidth() * Constants.SIMULATION_TIMESTEP_SEC);
//        }
//    }

    public double remaining_bw() {
        double remaining_bw = 0.0;
        for (int i = 0; i < net_graph_.nodes_.size(); i++) {
            for (int j = 0; j < net_graph_.nodes_.size(); j++) {
                if (links_[i][j] != null) {
                    remaining_bw += links_[i][j].remaining_bw();
                }
            }
        }

        return remaining_bw;
    }

/*    @Deprecated
    public HashMap<String, FlowGroup_Old_Compressed> schedule_flows(HashMap<String, Coflow_Old_Compressed> coflows,
                                                                    long timestamp) throws Exception {
        flows_.clear();
        reset_links();

        resetCFList(coflows);

        List<FlowGroup_Old_Compressed> flowList = scheduleRRF(timestamp);
        for (FlowGroup_Old_Compressed fgo : flowList){
            flows_.put(fgo.getId() , fgo);
        }

        return flows_;
    }*/

/*    public ArrayList<Map.Entry<Coflow_Old_Compressed, Double>> sort_coflows(HashMap<String, Coflow_Old_Compressed> coflows) throws Exception {
        HashMap<Coflow_Old_Compressed, Double> cct_map = new HashMap<Coflow_Old_Compressed, Double>();

        for (String k : coflows.keySet()) {
            Coflow_Old_Compressed c = coflows.get(k);
            MMCFOptimizer.MMCFOutput mmcf_out = MMCFOptimizer.glpk_optimize(c, net_graph_, links_);
            if (mmcf_out.completion_time_ != -1.0) {
                cct_map.put(c, mmcf_out.completion_time_);
            }
        }

        ArrayList<Map.Entry<Coflow_Old_Compressed, Double>> cct_list = new ArrayList<Map.Entry<Coflow_Old_Compressed, Double>>(cct_map.entrySet());
        Collections.sort(cct_list, new Comparator<Map.Entry<Coflow_Old_Compressed, Double>>() {
            public int compare(Map.Entry<Coflow_Old_Compressed, Double> o1, Map.Entry<Coflow_Old_Compressed, Double> o2) {
                if (o1.getValue() == o2.getValue()) return 0;
                return o1.getValue() < o2.getValue() ? -1 : 1;
            }
        });

        return cct_list;
    }*/


/*    // init the coflow and calculate the CCT under empty network
    // called upon adding a coflow
    @Deprecated
    public void coflowInit(Coflow_Old_Compressed cf) {

        // check if this cf has already finished
        boolean isEmpty = true;

        for (Map.Entry<String, FlowGroup_Old_Compressed> e : cf.flows.entrySet()) {
            if (e.getValue().remaining_volume() > 0 + Constants.DOUBLE_EPSILON) {
                isEmpty = false;
                break;
            }
        }

        if (isEmpty) {
            logger.error("Trying to init empty CF {}, skipping", cf.getId());
            return;
        }

        // call LP once to get the CCT_init
        MMCFOptimizer.MMCFOutput mmcf_out = null;
        try {

            mmcf_out = MMCFOptimizer.glpk_optimize(cf, net_graph_, linksAtStart); // LP when links are empty
            if (mmcf_out.completion_time_ != -1.0) { // If this Coflow is valid, add to a list
                cfseList.add(new CoflowSchedulerEntry(cf, mmcf_out));
            } else {
                logger.error("Unable to init CF {}, completion time = {}, fg_size {} , max volume {}", cf.toPrintableString(),
                        mmcf_out.completion_time_, cf.flows.size(), cf.flows.values().stream().max(Comparator.comparingDouble(s -> s.remaining_volume())).get().remaining_volume()
                );
//                System.exit(1); // don't fail
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/


    /**
     * update internal snapshot of Coflows. Called upon CF_FIN or FG_FIN (and anytime?)
     *
     * @param coflowPool
     */
    public void updateCFList(ConcurrentHashMap<String, Coflow> coflowPool) {
        // iterate through CFList, if non-existent, remove, else update the cct.
        ListIterator<CoflowSchedulerEntry> iter = cfseList.listIterator();

        while (iter.hasNext()) {
            CoflowSchedulerEntry cfse = iter.next();
            if (!coflowPool.containsKey(cfse.getCfID())) {
                iter.remove();
            } else {
                // if CF still exists in cfPool, then update the volume
                cfse.updateVolume(coflowPool.get(cfse.getCfID()));
            }
        }
    }

/*
    // uponFlowGroupFIN, we set the flowGroupVolume to zero.
    public void handleFlowGroupFIN(HashMap<String, Coflow_Old_Compressed> coflows) {
        // iterate through CFList, if non-existent, remove, else update the cct.
        ListIterator<CoflowSchedulerEntry> iter = cfseList.listIterator();

        while (iter.hasNext()) {
            CoflowSchedulerEntry cfse = iter.next();
            if (!coflows.containsKey(cfse.cf.getId())) {
                iter.remove();
            } else {
                Coflow_Old_Compressed cfo = coflows.get(cfse.cf.getId());
                // update the volume
                cfse.cf = cfo;
//                logger.info("HandleFGFIN on {} \n copied to {}", cfo.toPrintableString() , cfse.cf.toPrintableString());
            }

        }
    }

    // Upon finish of Coflow, we simply remove the CF from the list, and update the CCT
    public void handleCoflowFIN(HashMap<String, Coflow_Old_Compressed> coflows) {

        // iterate through CFList, if non-existent, remove, else update the cct.
        Iterator<CoflowSchedulerEntry> iter = cfseList.iterator();
        System.out.println("CF in scheduler: " + cfseList.size());

        while (iter.hasNext()) {
            CoflowSchedulerEntry cfse = iter.next();
            if (!coflows.containsKey(cfse.cf.getId())) {
                iter.remove();
            } else {
                Coflow_Old_Compressed cfo = coflows.get(cfse.cf.getId());
                // update the volume
                cfse.cf = cfo;
            }

        }
    }*/


    /**
     * RRF scheduling, given that CFs are already sorted. v2.0 ignore DDL
     *
     * @param timestamp
     * @return
     * @throws Exception
     */
    public HashMap<String, ScheduleOutputFG> scheduleRRF(long timestamp) throws Exception {
        HashMap<String, ScheduleOutputFG> scheduledFGs = new HashMap<>();
        LinkedList<CoflowSchedulerEntry> unscheduled_coflows = new LinkedList<>();

        reset_links();

/*        // API required by fanlai. Write current CCT to file
        BufferedWriter bwrt = new BufferedWriter(new java.io.FileWriter("/tmp/terra_coflows.txt"));
        bwrt.write(String.valueOf(System.currentTimeMillis()) + "\n");*/


        // Part 1: recursively schedule CFs in the priority queue
        for (CoflowSchedulerEntry cfse : cfseList) {

//            Coflow_Old_Compressed c = e.cf;

            // first fast check, we need to check every CF, even if we only have small BW left.
            if (!fastCheckCF(cfse)) {
                unscheduled_coflows.add(cfse);
                continue;
            }

            if (cfse != null && cfse.lastLPOutput != null) {
                logger.info("Last schedule: Coflow {} expected to complete in {} seconds", cfse.getCfID(), cfse.getLastLPOutput().completion_time_);
            }
//            bwrt.write(c.getId() + " " + e.cct + "\n");

            // schedule this CF
            MMCFOptimizer.MMCFOutput mmcf_out = MMCFOptimizer.glpk_optimizeNew(cfse, net_graph_, links_); // This is the recursive part.

            if (mmcf_out != null) {
                logger.info("This schedule: Coflow {} expected to complete in {} seconds", cfse.getCfID(), mmcf_out.completion_time_);
            } else {
                logger.info("This schedule: Coflow {} failed to run LP", cfse.getCfID());
            }

            boolean all_flows_scheduled = true;
            for (Map.Entry<String, CoflowSchedulerEntry.FlowGroupSchedulerEntry> fgseE : cfse.getFlowgroups().entrySet()) {
                CoflowSchedulerEntry.FlowGroupSchedulerEntry fgse = fgseE.getValue();
                if (mmcf_out.flow_link_bw_map_.get(fgse.intID) == null) {
                    all_flows_scheduled = false;
                    logger.warn("FG {} {}-{} / {}not being scheduled", fgse.fgID, fgse.srcLoc, fgse.dstLoc, fgse.remainingVol);
                }
            }
//            for (String k : c.flows.keySet()) {
//                FlowGroup_Old_Compressed f = c.flows.get(k);
//                if (!f.isDone()) {
//                    if (mmcf_out.flow_link_bw_map_.get(f.getInt_id()) == null) {
//                        all_flows_scheduled = false;
//                    }
//                }
//            }

            // check if successfully scheduled
            if (mmcf_out.completion_time_ == -1.0 || !all_flows_scheduled) {
                unscheduled_coflows.add(cfse);
                continue;
            }

            // Added update LPOutput part
            cfse.setLastLPOutput(mmcf_out);

            // This portion is similar to CoFlow::make() in Sim
            for (Map.Entry<String, CoflowSchedulerEntry.FlowGroupSchedulerEntry> fgseE : cfse.getFlowgroups().entrySet()) {

                CoflowSchedulerEntry.FlowGroupSchedulerEntry fgse = fgseE.getValue();

                // TODO(later) do we need to check whether the FG finished here?
/*                if (f.isDone()) {
                    continue;
                }*/

                ArrayList<Link> link_vals = mmcf_out.flow_link_bw_map_.get(fgse.intID);
                assert (link_vals != null);

                ScheduleOutputFG f = new ScheduleOutputFG(fgse);
                // This portion is similar to FlowGroup::make() in Sim
                make_paths(f, link_vals);

                // Subscribe the flow's paths to the links it uses
                for (Pathway p : f.paths) {
                    for (int i = 0; i < p.node_list.size() - 1; i++) {
                        int src = Integer.parseInt(p.node_list.get(i));
                        int dst = Integer.parseInt(p.node_list.get(i + 1));
                        links_[src][dst].subscribers_.add(p);
                    }
                }

/*                System.out.println("Adding flow " + f.getId() + " remaining = " + f.remaining_volume());
                System.out.println("  has pathways: ");
                for (Pathway p : f.paths) {
                    System.out.println("    " + p.toString());
                }*/


// No need for timestamp
/*
                if (f.getStart_timestamp() == -1) {
                    f.setStart_timestamp(timestamp);
                }
*/

//                flows_.put(f.getId(), f);
                scheduledFGs.put(f.getId(), f);
            }

        }

        // TODO(later) scheduleRemainFlows feature to be added
//        if (!unscheduled_coflows.isEmpty() && remaining_bw() > 0.0) {
//            scheduleRemainFlows(unscheduled_coflows, scheduledFGs, timestamp);
//        }

        return scheduledFGs;
    }

    /**
     * Schedule remaining flows
     *
     * @param unscheduled_coflows
     * @param timestamp
     */
    private void scheduleRemainFlows(LinkedList<CoflowSchedulerEntry> unscheduled_coflows, long timestamp) {
        LinkedList<CoflowSchedulerEntry.FlowGroupSchedulerEntry> unscheduled_fgse = new LinkedList<>();
        for (CoflowSchedulerEntry cfse : unscheduled_coflows) {

            // Filtering
            for (Map.Entry<String, CoflowSchedulerEntry.FlowGroupSchedulerEntry> fgseE : cfse.getFlowgroups().entrySet()) {
                if (fgseE.getValue().remainingVol > 0) {
                    unscheduled_fgse.add(fgseE.getValue());
                }
            }

        }
        // schedule from small to big
        Collections.sort(unscheduled_fgse, new Comparator<CoflowSchedulerEntry.FlowGroupSchedulerEntry>() {
            public int compare(CoflowSchedulerEntry.FlowGroupSchedulerEntry o1, CoflowSchedulerEntry.FlowGroupSchedulerEntry o2) {
                if (o1.remainingVol == o2.remainingVol) return 0;
                return o1.remainingVol < o2.remainingVol ? -1 : 1;
            }
        });

        for (CoflowSchedulerEntry.FlowGroupSchedulerEntry f : unscheduled_fgse) {
            int src = Integer.parseInt(f.srcLoc);
            int dst = Integer.parseInt(f.dstLoc);
            Pathway p = new Pathway(net_graph_.apsp_[src][dst]);

            double min_bw = Double.MAX_VALUE;
            SubscribedLink[] path_links = new SubscribedLink[p.node_list.size() - 1];
            for (int i = 0; i < p.node_list.size() - 1; i++) {
                int lsrc = Integer.parseInt(p.node_list.get(i));
                int ldst = Integer.parseInt(p.node_list.get(i + 1));
                SubscribedLink l = links_[lsrc][ldst];

                double bw = l.remaining_bw();
                path_links[i] = l;
                if (bw < min_bw) {
                    min_bw = bw;
                }
            }


            // TODO(later) implement make_paths() for scheduleRemainFlows later
/*            if (min_bw > 0) {
//                p.bandwidth = min_bw;
                p.setBandwidth(min_bw);

                for (SubscribedLink l : path_links) {
                    l.subscribers_.add(p);
                }
                f.paths.clear();
                f.paths.add(p);

*//*                System.out.println("Adding separate flow " + f.getId() + " remaining = " + f.remaining_volume());
                System.out.println("  has pathways: ");
                for (Pathway path : f.paths) {
                    System.out.println("    " + path.toString());
                }*//*

                if (f.getStart_timestamp() == -1) {
                    f.setStart_timestamp(timestamp);
                }
//                flows_.put(f.getId(), f);
                logger.info("Selected remaining flow: {}", f.getId());
                scheduledFGs.add(f);
            }*/
        }
    }

    public boolean fastCheckCF(CoflowSchedulerEntry cfse) {
        // check the FGs, if anyone of them can't be allocated, return false
        for (Map.Entry<String, CoflowSchedulerEntry.FlowGroupSchedulerEntry> e : cfse.getFlowgroups().entrySet()) {
            if (!fastCheckFG(e.getValue())) {
                return false;
            }
        }

        return true;
    }

    private boolean fastCheckFG(CoflowSchedulerEntry.FlowGroupSchedulerEntry fgse) {
        // check connectivity using link[][].remainingBW

        int srcID = Integer.parseInt(fgse.srcLoc);
        int dstID = Integer.parseInt(fgse.dstLoc);

        // BFS the graph from the source
        Queue<Integer> nodeIDtoCheck = new LinkedList<>();
        nodeIDtoCheck.add(srcID);

        Set<Integer> nodeIDDone = new HashSet<>();

        while (!nodeIDtoCheck.isEmpty()) {
            int curID = nodeIDtoCheck.remove();
            nodeIDDone.add(curID);

            // check nearby nodes from curID
            SubscribedLink[] nodeArray = links_[curID];
            if (nodeArray != null) {
                for (int j = 0; j < net_graph_.nodes_.size(); j++) {
                    if (links_[curID][j] != null) {
                        if (links_[curID][j].remaining_bw() >= Constants.LINK_AVAILABLE_THR) {

                            if (j == dstID) {
                                return true;
                            }

                            if (!nodeIDDone.contains(j)) {
                                nodeIDtoCheck.add(j);
                            }
                        }
                    }
                }
            } else {
                logger.error("nodeArray==null !");
            }
        }

        return false;
    }

/*    // sort first and then schedule
    public List<ScheduleOutputFG> scheduleRRFwithSort(long timestamp) throws Exception {
        sortCFList();
        return scheduleRRF(timestamp);
    }*/

    public void sortCFList() {
        cfseList.sort(smallCCTFirst);
    }

/*
    // called upon CF_ADD
    @Deprecated
    public void resetCFList(HashMap<String, Coflow_Old_Compressed> CFs) {
        cfseList.clear();
        for (Map.Entry<String, Coflow_Old_Compressed> entry : CFs.entrySet()) {
            coflowInit(entry.getValue());
            // fixed by checking in coflowInit()
        }

        sortCFList();
    }
*/

    /**
     * clear the snapshot and init every CF. Called upon CF_ADD
     *
     * @param cfPool
     */
    public void resetCFList(ConcurrentHashMap<String, Coflow> cfPool) {
        cfseList.clear();
        // Add all Coflows to cfseList, and snapshot them
        for (Map.Entry<String, Coflow> entry : cfPool.entrySet()) {
            Coflow cf = entry.getValue();
            // check if this cf has already finished
            boolean isEmpty = true;

            for (Map.Entry<String, FlowGroup> e : cf.getFlowGroups().entrySet()) {
                if (e.getValue().getRemainingVolume() > 0 + Constants.DOUBLE_EPSILON) {
                    isEmpty = false;
                    break;
                }
            }

            if (isEmpty) {
                logger.error("Trying to schedule empty CF {}, skipping", cf.getId());
                continue;
            }

            // Snapshot happens here
            CoflowSchedulerEntry cfe = new CoflowSchedulerEntry(cf);
            // add to cfseList here
            cfseList.add(cfe);
        }


        // After snapshot, iterate through them and get initial LP time
        // also prune the list of the CFs that can not be scheduled
        ListIterator<CoflowSchedulerEntry> iter = cfseList.listIterator();
        while (iter.hasNext()) {
            CoflowSchedulerEntry cfse = iter.next();

            // call LP once to get the CCT_init
            MMCFOptimizer.MMCFOutput mmcf_out = null;
            try {

                mmcf_out = MMCFOptimizer.glpk_optimizeNew(cfse, net_graph_, linksAtStart); // LP when links are empty
                if (mmcf_out.completion_time_ != -1.0) { // If this Coflow is valid, add the results

//                    cfseList.add(new CoflowSchedulerEntry(cf, mmcf_out));
                } else {
                    logger.error("Unable to init CF {}, completion time = {}, fg_size {} ", cfse.getCoflow().toPrintableString(),
                            mmcf_out.completion_time_, cfse.getFlowgroups().size());
                    iter.remove(); // remove from CF List
//                System.exit(1); // don't fail
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        sortCFList();
    }


    // print the CFList that is going into the scheduler
    public void printCFList() {
        StringBuilder str = new StringBuilder("-----CF List-----\n");

        for (CoflowSchedulerEntry cfe : cfseList) {
            Coflow cf = cfe.getCoflow();
            str.append(cf.getId()).append(' ').append(cfe.lastLPOutput.completion_time_).append('\n');
            for (Map.Entry<String, FlowGroup> fge : cf.getFlowGroups().entrySet()) {
                FlowGroup fg = fge.getValue();
                str.append(' ').append(fge.getKey()).append(' ').append(fg.getFlowGroupState())
                        .append(' ').append(fg.getRemainingVolume()).append('/').append(fg.getTotalVolume()).append('\n');
            }
        }
        logger.info(str);
    }

    /*public void schedule_extra_flows(ArrayList<Coflow_Old_Compressed> unscheduled_coflows, long timestamp) {
        ArrayList<FlowGroup_Old_Compressed> unscheduled_flowGroups = new ArrayList<FlowGroup_Old_Compressed>();
        for (Coflow_Old_Compressed c : unscheduled_coflows) {
            for (String k : c.flows.keySet()) {
                FlowGroup_Old_Compressed f = c.flows.get(k);
                if (f.remaining_volume() > 0) {
                    unscheduled_flowGroups.add(c.flows.get(k));
                }
            }
        }
        Collections.sort(unscheduled_flowGroups, new Comparator<FlowGroup_Old_Compressed>() {
            public int compare(FlowGroup_Old_Compressed o1, FlowGroup_Old_Compressed o2) {
                if (o1.getRemainingVolume() == o2.getRemainingVolume()) return 0;
                return o1.getRemainingVolume() < o2.getRemainingVolume() ? -1 : 1;
            }
        });

        for (FlowGroup_Old_Compressed f : unscheduled_flowGroups) {
            int src = Integer.parseInt(f.getSrc_loc());
            int dst = Integer.parseInt(f.getDst_loc());
            Pathway p = new Pathway(net_graph_.apsp_[src][dst]);

            double min_bw = Double.MAX_VALUE;
            SubscribedLink[] path_links = new SubscribedLink[p.node_list.size() - 1];
            for (int i = 0; i < p.node_list.size() - 1; i++) {
                int lsrc = Integer.parseInt(p.node_list.get(i));
                int ldst = Integer.parseInt(p.node_list.get(i+1));
                SubscribedLink l = links_[lsrc][ldst];

                double bw = l.remaining_bw();
                path_links[i] = l;
                if (bw < min_bw) {
                    min_bw = bw;
                }
            }

            if (min_bw > 0) {
//                p.bandwidth = min_bw;
                p.setBandwidth( min_bw);

                for (SubscribedLink l : path_links) {
                    l.subscribers_.add(p);
                }
                f.paths.clear();
                f.paths.add(p);

*//*                System.out.println("Adding separate flow " + f.getId() + " remaining = " + f.remaining_volume());
                System.out.println("  has pathways: ");
                for (Pathway path : f.paths) {
                    System.out.println("    " + path.toString());
                }*//*

                if (f.getStart_timestamp() == -1) {
                    f.setStart_timestamp(timestamp);
                }
                flows_.put(f.getId(), f);
            }
        }
    }*/
}
