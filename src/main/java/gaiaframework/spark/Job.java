package gaiaframework.spark;

import java.util.ArrayList;
import java.util.HashMap;

import gaiaframework.network.Coflow_Old;

// A job within a trace
public class Job {

    private String id;
    private long arrivalTime;
    public HashMap<String, Coflow_Old> coflows = new HashMap<String, Coflow_Old>();
    public ArrayList<Coflow_Old> root_coflows = new ArrayList<Coflow_Old>();
    public ArrayList<Coflow_Old> leaf_coflows = new ArrayList<Coflow_Old>();
    private boolean started = false;
    private long start_timestamp = -1;
    private long end_timestamp = -1;

    // Coflows that are currently running
    public ArrayList<Coflow_Old> running_coflows = new ArrayList<Coflow_Old>();

    // Coflows taht are ready to begin but have not begun yet
    public ArrayList<Coflow_Old> ready_coflows = new ArrayList<Coflow_Old>();

    public Job(String id, long start_time, HashMap<String, Coflow_Old> coflows) {
        this.id = id;
        this.coflows = coflows;
        arrivalTime = start_time;

        // Determine the end coflows of the DAG (those without any parents).
        // Determine the start coflows of the DAG (those without children).
        for (String key : this.coflows.keySet()) {
            Coflow_Old c = this.coflows.get(key);
            if (c.parent_coflows.size() == 0) {
                leaf_coflows.add(c);
            }

            if (c.child_coflows.size() == 0) {
                root_coflows.add(c);
            }
            else {
                // Flows are created by depedent coflows. Since
                // starting coflows do not depend on anyone, they 
                // should not create coflows.
                c.create_flows();
            }
        }
    }

    // A job is considered done if all of its coflows are done
    public boolean done() {
        for (String k : coflows.keySet()) {
            Coflow_Old c = coflows.get(k);
            if (!(c.done() || c.flows.isEmpty())) {
                return false;
            }
        }

        return true;
    }

    // Remove s all of the parent Coflows depending on it. If any parent
    // Coflows are now ready to run, add them to ready_coflows.
    public void finish_coflow(String full_coflow_id) {
        // A coflow's id is of the form <job_id>:<coflow_id> whereas
        // our coflow map is indxed by <coflow_id>. Retrieve the coflow_id here.
        String coflow_id = full_coflow_id.split(":")[1];
        Coflow_Old c = coflows.get(coflow_id);
        running_coflows.remove(c);

        for (Coflow_Old parent : c.parent_coflows) {
            if (parent.ready()) {
                ready_coflows.add(parent);
            }

        } // for parent_coflows

    }

    // Transition all ready coflows to running and return a list of all
    // coflows that are currently running for this job.
    public ArrayList<Coflow_Old> get_running_coflows() {
        running_coflows.addAll(ready_coflows);
        ready_coflows.clear();
       
        // Return a clone of the list so that the calling function
        // can call finish_coflow while iterating over this list.
        return (ArrayList<Coflow_Old>) running_coflows.clone();
    }

    // Start all of the first coflows of the job
    public void start() {
        for (Coflow_Old c : root_coflows) {
            c.setDone(true);
            // Coflows are defined from parent stage to child stage,
            // so we add the start stage's parents first.
            for (Coflow_Old parent : c.parent_coflows) {
                if (!ready_coflows.contains(parent)) {
                    if (parent.done()) {
                        // Hack to avoid error in finish_coflow
                        running_coflows.add(parent);
                        finish_coflow(parent.getId());
                    }
                    // Add coflows which can be scheduled as a whole
                    else if (parent.ready()) {
                        ready_coflows.add(parent);
                    }
                }
            } // for parent_coflows

        } // for root_coflows
        started = true;
    }

    ///// getters and setters /////
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(long arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    public boolean isStarted() {
        return started;
    }

    public void setStarted(boolean started) {
        this.started = started;
    }

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

}
