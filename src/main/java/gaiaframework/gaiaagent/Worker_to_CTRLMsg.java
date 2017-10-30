package gaiaframework.gaiaagent;

import gaiaframework.gaiaprotos.GaiaMessageProtos;

public class Worker_to_CTRLMsg {
    public enum  MsgType {
        FLOWSTATUS,
        PATHSTATUS
    }

    public MsgType type;

    public GaiaMessageProtos.FlowStatusReport flowStatusReport;

    public GaiaMessageProtos.PathStatusReport pathStatusReport;

    public Worker_to_CTRLMsg(GaiaMessageProtos.FlowStatusReport statusReport){
        this.flowStatusReport = statusReport;
        this.type = MsgType.FLOWSTATUS;
    }

    public Worker_to_CTRLMsg(GaiaMessageProtos.PathStatusReport statusReport){
        this.pathStatusReport = statusReport;
        this.type = MsgType.PATHSTATUS;
    }
}
