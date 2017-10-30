package gaiaframework.network;

import java.util.ArrayList;
import java.util.HashMap;

// A coflow represents a (set of) shuffles within a job. It is one or more edges within a DAG.
// It is defined as a set of shuffles with a common destination stage.
// We use the destination stage as an anchor(key) for this coflow.
public class Coflow_Old {

    public int ddl_Millis = -1;

    public String getId() { return id; }

    public String id;
    public HashMap<String, FlowGroup_Old> flows = new HashMap<String, FlowGroup_Old>();
    public double volume_ = 0.0;
    public long start_timestamp = -1;

    public long getStart_timestamp() {
        return start_timestamp;
    }

    public void setStart_timestamp(long start_timestamp) {
        this.start_timestamp = start_timestamp;
    }

    public long getEnd_timestamp() {
        return end_timestamp;
    }

    public void setEnd_timestamp(long end_timestamp) {
        this.end_timestamp = end_timestamp;
    }

    public long end_timestamp = -1;
    public boolean done = false;

    // The location of coflow-initiating tasks. For example, these would be
    // the locations of map tasks in a map-reduce shuffle.
    public String[] task_locs_;

    public void setDone(boolean done) {
        this.done = done;
    }

    // Coflows that this coflow depends on (must complete before this
    // coflow starts).
    public ArrayList<Coflow_Old> child_coflows = new ArrayList<Coflow_Old>();

    // Coflows which depend on this Coflow (this Coflow must complete
    // before parent Coflows start).
    public ArrayList<Coflow_Old> parent_coflows = new ArrayList<Coflow_Old>();

    // The volume to be shuffled to parent coflow, keyed by parent coflow id
    public HashMap<String, Double> volume_for_parent = new HashMap<String, Double>();

    public Coflow_Old(String id, String[] task_locs) {
        this.id = id;
        task_locs_ = task_locs;
    }

    public void create_flows() {
        volume_ = 0.0;

        String flow_id_prefix = id + ":";
        int flow_id_suffix = 0;

        // This shuffle transmits data to other tasks in the DAG. Tasks are
        // grouped together into the shuffles resulting from them.
        for (Coflow_Old child : child_coflows) {

            // A child will have tasks in multiple locations. We assume that
            // there is one flow between each pair of locations within our
            // task set and the child's task set and that these transfers
            // are all of the same size. Note that flows go from
            // child_task -> our_task.
            int num_flows = task_locs_.length * child.task_locs_.length;
            double volume_per_flow = child.volume_for_parent.get(id) / (double)num_flows;
            for (String src_loc : child.task_locs_) {

                for (String dst_loc : task_locs_) {

                    // If the src and dst locations are the same, no network
                    // transmission is needed, so we don't create a flow.
                    if (src_loc != dst_loc) {
                        String flow_id = flow_id_prefix + flow_id_suffix;
                        flows.put(flow_id, new FlowGroup_Old(flow_id, flow_id_suffix, id, src_loc, dst_loc, volume_per_flow));
//                        System.out.println("CoFlow: created flow id: " + flow_id + " owned by coflow id: " + id + " with volume " + volume_per_flow);
                        volume_ += volume_per_flow;
                        flow_id_suffix++;
                    }
                    else {
                        System.out.println("Skipping because src and dst are same " + src_loc + " " + dst_loc + " " + child.id + "->" + id);
                    }

                } // task_locs_

            } // for child.task_locs_

        } // for child_coflows_

    }

    // Sets the coflow's start time to be that of the earliest starting flow.
    // Assumes all flows are done.
    public void determine_start_time() {
        // TODO: what if there are no flows. creating a new function.
        start_timestamp = Long.MAX_VALUE;
        for (String k : flows.keySet()) {
            FlowGroup_Old f = flows.get(k);
            if (f.getStart_timestamp() < start_timestamp) {
                start_timestamp = f.getStart_timestamp();
            }
        }
    }

    // TODO: recapsuling the CoFlow Class and provide the proper interface.
    public long getCoFlowStartTime() {
        return 0;
    }

    // Return whether owned Flows are done
    public boolean done() {
        if (!done) {
            for (String k : flows.keySet()) {
                if (!flows.get(k).isDone()) {
                    return false;
                }
            }
            done = true;
        }
        return true;
    }

    // Returns whether the Coflow can begin or not. A Coflow can begin
    // only if all of the Coflows on which it depends have completed.
    public boolean ready() {
        for (Coflow_Old s : child_coflows) {
            if (!s.done) {
                return false;
            }
        }

        return true;
    }
}

