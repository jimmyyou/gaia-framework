package gaiaframework.scheduler;

import java.util.HashMap;

import gaiaframework.network.Coflow_Old_Compressed;
import gaiaframework.network.FlowGroup_Old_Compressed;
import gaiaframework.network.SubscribedLink;
import gaiaframework.network.NetGraph;

import org.graphstream.graph.Edge;

public abstract class Scheduler {
    public NetGraph net_graph_;

    // All possible links in our graph
    public SubscribedLink[][] links_;

    public Scheduler(NetGraph net_graph) {
        net_graph_ = net_graph;

        links_ = new SubscribedLink[net_graph_.nodes_.size()][net_graph_.nodes_.size()];
        for (Edge e : net_graph_.graph_.getEachEdge()) {
            int src = Integer.parseInt(e.getNode0().toString());
            int dst = Integer.parseInt(e.getNode1().toString());
            links_[src][dst] = new SubscribedLink(Double.parseDouble(e.getAttribute("bandwidth").toString()));
            links_[dst][src] = new SubscribedLink(Double.parseDouble(e.getAttribute("bandwidth").toString()));
        }
    }
    
    public abstract void finish_flow(FlowGroup_Old_Compressed f);

    public abstract void progress_flow(FlowGroup_Old_Compressed f);

    public void reset_links() {
        for (int i = 0; i < net_graph_.nodes_.size(); i++) {
            for (int j = 0; j < net_graph_.nodes_.size(); j++) {
                if (links_[i][j] != null) {
                    links_[i][j].subscribers_.clear();
                }
            }
        }
    }

    public abstract HashMap<String, FlowGroup_Old_Compressed> schedule_flows(HashMap<String, Coflow_Old_Compressed> coflows,
                                                                             long timestamp) throws Exception;

    public abstract void update_flows(HashMap<String, FlowGroup_Old_Compressed> flows);
}
