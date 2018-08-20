package gaiaframework.gaiaagent;

// Message for subscribe a FlowGroup to a persistent connection (worker).

import gaiaframework.transmission.DataChunkMessage;

public class CTRL_to_WorkerMsg {
    public enum  MsgType {
        SUBSCRIBE,
        UNSUBSCRIBE,
        CONNECT,
        DATA,
        SYNC // SYNC do not carry information
    }

    MsgType type;

//    AggFlowGroupInfo fgi;
    double rate = 0.0;
    boolean pause = false;
    DataChunkMessage dataChunkMessage;

//    // subscribe or change rate.
//    public CTRL_to_WorkerMsg(AggFlowGroupInfo fgi, double rate) {
//        this.type = MsgType.SUBSCRIBE;
//        this.fgi = fgi;
//        this.rate = rate;
//        this.pause = false;
//    }
//
//    // unsubscribe
//    public CTRL_to_WorkerMsg(AggFlowGroupInfo fgi) {
//        this.type = MsgType.UNSUBSCRIBE;
//        this.fgi = fgi;
//        this.rate = 0.0;
//        this.pause = true;
//    }

    public CTRL_to_WorkerMsg(int NULL){
        this.type = MsgType.CONNECT;
    }

    public CTRL_to_WorkerMsg(){
        this.type = MsgType.SYNC;
    }

    public CTRL_to_WorkerMsg(DataChunkMessage data) {
        this.dataChunkMessage = data;
        this.type = MsgType.DATA;
    }

    public MsgType getType() { return type; }

//    public AggFlowGroupInfo getFgi() { return fgi; }

    public double getRate() { return rate; }

    public boolean isPause() { return pause; }

}
