package gaiaframework.comm;

import java.io.Serializable;

// Messages being sent from SendingAgent to SendingAgentContact
// regarding the port numbers used by connections between the
// SendingAgent and various ReceivingAgents.
public class PortAnnouncementMessage_Old implements Serializable {
    public String sa_id_;
    public String ra_id_;
    public int path_id_;
    public int port_no_; // Port number used by SendingAgent in connecting
                         // to a ReceivingAgent on path path_id_

    public PortAnnouncementMessage_Old(String sa_id, String ra_id,
                                       int path_id, int port_no) {
        sa_id_ = sa_id;
        ra_id_ = ra_id;
        path_id_ = path_id;
        port_no_ = port_no;
    }
}
