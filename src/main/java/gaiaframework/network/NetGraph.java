package gaiaframework.network;

import org.graphstream.algorithm.APSP;
import org.graphstream.algorithm.APSP.APSPInfo;
import org.graphstream.graph.*;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.file.FileSource;
import org.graphstream.stream.file.FileSourceFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import gaiaframework.util.Constants;

public class NetGraph {
    public Graph graph_;
    public ArrayList<String> nodes_ = new ArrayList<String>();
    public HashMap<String, String> trace_id_to_node_id_ = new HashMap<String, String>();

    // All pairs shortest path. First index is src node, second index
    // is dst node.
    public Path[][] apsp_;

    // All pairs most bandwidth (paths between each node which have the greatest
    // minimum link bandwidth).
    public Pathway[][] apmb_;

    // All pairs all paths. First index is src node, second index is
    // dst node, resulting in a list of all paths used between
    // the src and dst node.
    public HashMap<String, HashMap<String, ArrayList<Pathway>>> apap_;

    // Interface between each switch
    public HashMap<String, HashMap<String, String>> interfaces_;

    public int total_num_paths_ = 0;

    // Max bandwidth of each link
    public Double[][] link_bw_;

    public NetGraph(String gml_file, double bw_factor) throws java.io.IOException {
        graph_ = new SingleGraph("gaiaframeworkGraph");
        
        FileSource fs = FileSourceFactory.sourceFor(gml_file);
        fs.addSink(graph_);

        fs.readAll(gml_file);
        fs.removeSink(graph_);

        Constants.node_id_to_trace_id = new HashMap<String, String>();
        interfaces_ = new HashMap<String, HashMap<String, String>>();
        HashMap<String, Integer> max_interface_numbers = new HashMap<String, Integer>();
        for (Node n:graph_) {
            nodes_.add(n.toString());
            trace_id_to_node_id_.put(n.getLabel("ui.label").toString(), n.toString());
            Constants.node_id_to_trace_id.put(n.toString(), n.getLabel("ui.label").toString());

            // Set up the interface table and assign the interface between this node
            // and itself (actually the interface between this node's switch and its host)
            // to be 1.
            interfaces_.put(n.toString(), new HashMap<String, String>());
            interfaces_.get(n.toString()).put(n.toString(), "1");
            max_interface_numbers.put(n.toString(), 1);
        }

        link_bw_ = new Double[nodes_.size()][nodes_.size()];
        for (Edge e : graph_.getEachEdge()) {
            String src_str = e.getNode0().toString();
            String dst_str = e.getNode1().toString();
            int src = Integer.parseInt(src_str);
            int dst = Integer.parseInt(dst_str);
            // multiply the bandwidth by scale factor
            e.setAttribute("bandwidth", Double.parseDouble(e.getAttribute("bandwidth").toString()) * bw_factor );
            link_bw_[src][dst] = Double.parseDouble(e.getAttribute("bandwidth").toString());
            link_bw_[dst][src] = Double.parseDouble(e.getAttribute("bandwidth").toString());
        }


        // Set up interface table
        Edge e = null;
        for (int i = 0; i < nodes_.size(); i++) {
            String src = nodes_.get(i);
            for (int j = i + 1; j < nodes_.size(); j++) {
                String dst = nodes_.get(j);
                if ((e = graph_.getNode(src).getEdgeFrom(dst)) != null) {
                    int src_if = max_interface_numbers.get(src) + 1;
                    max_interface_numbers.put(src, src_if);
                    interfaces_.get(src).put(dst, Integer.toString(src_if));

                    int dst_if = max_interface_numbers.get(dst) + 1;
                    max_interface_numbers.put(dst, dst_if);
                    interfaces_.get(dst).put(src, Integer.toString(dst_if));
                }
            }
        }
        
        APSP apsp = new APSP();
        apsp.init(graph_);
        apsp.setDirected(false);
        apsp.compute();
       
        // Since we'll be indexing this array by nodeID, and nodeID's start
        // at 1, we need num_nodes+1 entries in the array.
        apsp_ = new Path[nodes_.size()][nodes_.size()];
        apmb_ = new Pathway[nodes_.size()][nodes_.size()];
        apap_ = new HashMap<String, HashMap<String, ArrayList<Pathway>>>();

        for (Node n : graph_) {
            APSPInfo info = n.getAttribute(APSPInfo.ATTRIBUTE_NAME);
            apap_.put(n.toString(), new HashMap<String, ArrayList<Pathway>>());

            for (Node n_ : graph_) {
                if (!n.toString().equals(n_.toString())) {
                    int src = Integer.parseInt(n.toString());
                    int dst = Integer.parseInt(n_.toString());
                    apsp_[src][dst] = info.getShortestPathTo(n_.toString());

                    ArrayList<Pathway> paths = make_paths(n, n_);
                    total_num_paths_ += paths.size();
                    apap_.get(n.toString()).put(n_.toString(), paths);
                    Pathway max_bw_path = new Pathway();
                    for (Pathway p : paths) {
                        assign_bw(p);
                        if (p.getBandwidth() > max_bw_path.getBandwidth()) {
                            max_bw_path = p;
                        }
                    }
                    
                    apmb_[src][dst] = max_bw_path;
                }

            } // for n_

        } // for n
    }

    // Find the minimum link bandwidth available on p. This is the bandwidth available
    // on the pathway.
    private void assign_bw(Pathway p) {
        double min_bw = Double.MAX_VALUE;
        for (int i = 0; i < p.node_list.size() - 1; i++) {
            int src = Integer.parseInt(p.node_list.get(i));
            int dst = Integer.parseInt(p.node_list.get(i+1));
            double bw = link_bw_[src][dst];

            if (bw < min_bw) {
                min_bw = bw;
            }
        }

//        p.bandwidth = min_bw;
        p.setBandwidth( min_bw);
    }

    // Returns the id of the path with matching node_list to p
    public int get_path_id(Pathway p) {
        int path_id = 0;
        for (Pathway other : apap_.get(p.src()).get(p.dst())) {
            if (p.equals(other)) {
                return path_id;
            }
            path_id++;
        }
        return -1;
    }

    // Make all paths from src to dst
    private ArrayList<Pathway> make_paths(Node src, Node dst) {
        ArrayList<Pathway> pathways = new ArrayList<Pathway>();
        Pathway p = new Pathway();
        p.node_list.add(src.toString());
        make_paths_helper(src, dst.toString(), p, pathways);
        return pathways;
    }

    // Perform depth-first-search to find all paths from cur to dst
    private void make_paths_helper(Node cur, String dst, Pathway cur_path, ArrayList<Pathway> pathways) {
        Iterator<Node> neighbor_it = cur.getNeighborNodeIterator();
        while (neighbor_it.hasNext()) {
            Node n = neighbor_it.next();
            String n_str = n.toString();
            if (!cur_path.node_list.contains(n_str)) {
                cur_path.node_list.add(n_str);
                if (dst.equals(n_str)) {
                    pathways.add(new Pathway(cur_path));
                }
                else {
                    make_paths_helper(n, dst, cur_path, pathways);
                }
                
                cur_path.node_list.remove(n_str);

            } // if neighbor not in path

        } // while hasNext
    }

}
