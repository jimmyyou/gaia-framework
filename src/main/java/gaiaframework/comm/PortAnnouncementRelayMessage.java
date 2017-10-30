package gaiaframework.comm;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.util.concurrent.LinkedBlockingQueue;

import gaiaframework.network.NetGraph;
import gaiaframework.network.Pathway;

// Relays PortAnnouncementMessages from SendingAgents
// to the OpenFlow controller that will create FlowMods
// to set up paths.
public class PortAnnouncementRelayMessage {

    public NetGraph net_graph_;
    public LinkedBlockingQueue<PortAnnouncementMessage_Old> port_announcements_;
    
    public PortAnnouncementRelayMessage(NetGraph net_graph,
                                        LinkedBlockingQueue<PortAnnouncementMessage_Old> port_announcements) {
        net_graph_ = net_graph;
        port_announcements_ = port_announcements;
    }

    public void relay_ports() {
        int num_ports_recv = 0;
        String announcement;
       
        try {
            // Set up the fifo that we'll receive from
            Runtime rt = Runtime.getRuntime();
            String recv_fifo_name = "/tmp/gaia_fifo_to_ctrl";
            rt.exec("rm " + recv_fifo_name).waitFor();

            // Set up the fifo that we'll write to
            File f = new File("/tmp/gaia_fifo_to_of");
            FileWriter fw = new FileWriter(f);
            BufferedWriter bw = new BufferedWriter(fw);

            // Set the file permissions on the fifo to 666 (anyone rw) because
            // the emulated controller is run as root (so that it can be started
            // from mininet). If we don't set the file permissions here, the
            // fifo can only be accessed as root.
            rt.exec("mkfifo " + recv_fifo_name + " -m 666").waitFor();

            // Send the number of port announcements that will be
            // sent to the OF controller.
            bw.write(Integer.toString(net_graph_.total_num_paths_) + '\n');
            bw.flush();

            int msg_id = 0;
            // As we receive port announcements, send the information needed
            // by the OF controller to set FlowMods for the paths.
            while (num_ports_recv < net_graph_.total_num_paths_) {
                PortAnnouncementMessage_Old m = port_announcements_.take();
                announcement = "Received port <" + m.sa_id_ + ", " + m.ra_id_ + ", " + m.path_id_ + ", " + m.port_no_ + ">";

                Pathway p = net_graph_.apap_.get(m.sa_id_).get(m.ra_id_).get(m.path_id_);
                int num_messages = p.node_list.size() * 2;

                // Metadata is of form:
                //      msg_id num_rules src_id dst_id src_port dst_port
                //      
                // msg_id:      used to keep track of how many rules the OF controller will set
                // num_rules:   how many rules will be set for this msg_id
                // src_id:      id of path source
                // dst_id:      id of path destination
                // src_port:    port number used by sending agent
                // dst_port:    port number used by receiving agent (should be 33330)
//                String metadata = Integer.toString(msg_id) + ' ' + Integer.toString(num_messages) + ' ' + p.src() + ' ' + p.dst() + ' ' + Integer.toString(m.port_no_) + " 33330\n";
                // Fixed: If we make ID starts with 0, then we need to add 1 here
                String metadata = Integer.toString(msg_id) + ' ' + Integer.toString(num_messages) + ' ' + (Integer.parseInt(p.src())+1) + ' ' +
                        (Integer.parseInt(p.dst())+1) + ' ' + Integer.toString(m.port_no_) + " 33330\n";
//                System.out.println("sending metadata: " + metadata + "for path: " + p.toString());
                bw.write(metadata);

                // Individual messages are of form:
                //      msg_id dpid out_port fwd_or_rev
                // 
                // dpid:        id of switch to be programmed
                // out_port:    interface through which packets should be forwarded
                // fwd_or_rev:  0 means this rule is for the forward direction,
                //              1 means for the reverse direction
                //              If on reverse direction, src_{id, port} should be
                //              switched with dst_{ip, port}.
                String src, dst, out_port;
                String message;

                // Set the forward path
                for (int i = 0; i < p.node_list.size(); i++) {
                    src = p.node_list.get(i);

                    if (i < p.node_list.size() - 1) {
                        dst = p.node_list.get(i+1);
                    }
                    else {
                        dst = src;
                    }
                   
                    out_port = net_graph_.interfaces_.get(src).get(dst);
                    message = Integer.toString(msg_id) + ' ' + (Integer.parseInt(src) + 1) + ' ' + out_port + " 0\n";
//                    System.out.print("    " + message);
                    bw.write(message);
                }

                // Set the reverse path
                for (int i = p.node_list.size() - 1; i >= 0; i--) {
                    src = p.node_list.get(i);

                    if (i > 0) {
                        dst = p.node_list.get(i-1);
                    }
                    else {
                        dst = src;
                    }

                    out_port = net_graph_.interfaces_.get(src).get(dst);
                    message = Integer.toString(msg_id) + ' ' + (Integer.parseInt(src) + 1) + ' ' + out_port + " 1\n";
//                    System.out.print("    " + message);
                    bw.write(message);
                }
                bw.flush();

                msg_id++;
                num_ports_recv++;
            }
            bw.close();

            // We've sent all of the rules that the OF controller needs to process.
            // We must wait for the OF controller to finish setting rules before
            // continuing on. Open up another fifo to wait for a '1' from the
            // OF controller.
            f = new File(recv_fifo_name);
            FileReader fr = new FileReader(f);
            int status = fr.read();

            if (status != '1') {
                System.out.println("ERROR: Received unexpected return status " + status + " from OF controller");
                System.exit(1);
            }
            System.out.println("All rules set");
            fr.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error while setting up flow rules");
            System.exit(1);
        }

    }

}
