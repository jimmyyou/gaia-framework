package gaiaframework.network;

import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.List;

import org.graphstream.graph.*;

import gaiaframework.util.Constants;

public class Pathway {
    public ArrayList<String> node_list = new ArrayList<String>();

    public double getBandwidth() {
        return bandwidth;
    }

    public void setBandwidth(double bandwidth) {
        this.bandwidth = bandwidth;
    }

    private double bandwidth = 0.0;

    public Pathway() {}

    public Pathway(Path p) {
        List<Node> path_nodes = p.getNodePath();
        for (Node n : path_nodes) {
            node_list.add(n.toString());
        }
    }

    public Pathway(Pathway p) {
        bandwidth = p.bandwidth;
        node_list = new ArrayList<String>(p.node_list);
    }

    // Returns the first node in the node_list
    public String src() {
        return node_list.get(0);
    }

    // Returns the last node in the node_list
    public String dst() {
        return node_list.get(node_list.size() - 1);
    }

    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("[ ");
        for (String s : node_list) {
            str.append(Constants.node_id_to_trace_id.get(s) + ", ");
        }
        str.append("] " + bandwidth);
        return str.toString();
    }

    // Returns whether this Pathway has an equivalent node_list as other.
    public boolean equals(Pathway other) {
        for (int i = 0; i < node_list.size(); i++) {
            if (!node_list.get(i).equals(other.node_list.get(i))) {
                return false;
            }
        }
        return true;
    }
}
