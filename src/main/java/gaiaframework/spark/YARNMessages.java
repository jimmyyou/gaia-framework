package gaiaframework.spark;

// This is YARNMessages. Intended for communication between the DAGReader and Coflow_Old Inserter.
// We don't need multiple classes for these messages, because they are small and simple.

public class YARNMessages {

    public enum Type{
        DAG_ARRIVAL,
        END_OF_INCOMING_JOBS,
        COFLOW_FIN,
        COFLOW_DROP
    }
    private Type type;

    // coflow ID for COFLOW_FIN and COFLOW_DROP
    public String coflow_ID;

    // DAG for DAG_ARRIVAL
//    public DAG arrivedDAG;

    // default msg: END_OF_INCOMING_JOBS
    public YARNMessages(){
        this.type = Type.END_OF_INCOMING_JOBS;
    }

//    public YARNMessages(DAG arrivedDAG){
//        this.type = Type.DAG_ARRIVAL;
//        this.arrivedDAG = arrivedDAG;
//    }

    public YARNMessages(String FIN_coflow_ID){
        this.type = Type.COFLOW_FIN;
        this.coflow_ID = FIN_coflow_ID;
    }

    public YARNMessages(String DROP_coflow_ID, Type t){
        this.type = t;
        this.coflow_ID = DROP_coflow_ID;
    }

    public Type getType() { return type; }
}
