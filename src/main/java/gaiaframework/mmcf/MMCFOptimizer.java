package gaiaframework.mmcf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import gaiaframework.network.*;
import gaiaframework.network.Coflow_Old;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MMCFOptimizer {
    private static final Logger logger = LogManager.getLogger();

    public static class MMCFOutput {
        public double completion_time_ = 0.0;
        public HashMap<Integer, ArrayList<Link>> flow_link_bw_map_ 
            = new HashMap<Integer, ArrayList<Link>>();
    }

    public static MMCFOutput glpk_optimize(Coflow_Old coflow, NetGraph net_graph, SubscribedLink[][] links) throws Exception {
        String path_root = "/tmp";
        String mod_file_name = path_root + "/MinCCT.mod";
        StringBuilder dat_string = new StringBuilder();
        dat_string.append("data;\n\n");

        dat_string.append("set N:=");
        for (int i = 0; i < net_graph.nodes_.size(); i++) {
            dat_string.append(" " + i);
        }
        dat_string.append(";\n");

        ArrayList<Integer> flow_int_id_list = new ArrayList<Integer>();
        HashMap<Integer, String> flow_int_id_to_id = new HashMap<Integer, String>();
//        System.out.println("Coflow_Old " + coflow.getId() + " has flows: ");
        for (String k : coflow.flows.keySet()) {
            FlowGroup_Old f = coflow.flows.get(k);
            if (f.remaining_volume() > 0.0) {
//                System.out.println("  " + k + ": " + f.getSrc_loc() + "-" + f.getDst_loc() + " -> " + f.remaining_volume());
                int int_id = coflow.flows.get(k).getInt_id();
                flow_int_id_list.add(int_id);
                flow_int_id_to_id.put(int_id, k);
            }
        }
        Collections.sort(flow_int_id_list);

        dat_string.append("set F:=");
        for (int fid : flow_int_id_list) {
            dat_string.append(" f" + fid); // NOTE: Original mmcf did fid-1
        }
        dat_string.append(";\n\n");
        
        dat_string.append("param b:\n");
        for (int i = 0; i < net_graph.nodes_.size(); i++) {
            dat_string.append(i + " " );
        }
        dat_string.append(":=\n");
        for (int i = 0; i < net_graph.nodes_.size(); i++) {
            dat_string.append(i + " ");
            for (int j = 0; j < net_graph.nodes_.size(); j++) {
                if (i == j || links[i][j] == null) {
                    dat_string.append(" 0.000");
                }
                else {
                    dat_string.append(String.format(" %.3f", links[i][j].remaining_bw() / 8)); // convert B/W to Byte
                }
            }
            dat_string.append("\n");
        }
        dat_string.append(";\n\n");

        dat_string.append("param fs:=\n");
        for (int fid : flow_int_id_list) {
            String flow_id = flow_int_id_to_id.get(fid);
            dat_string.append(" f" + fid + " " + coflow.flows.get(flow_id).getSrc_loc() + "\n");
        }
        dat_string.append(";\n\n");

        dat_string.append("param fe:=\n");
        for (int fid : flow_int_id_list) {
            String flow_id = flow_int_id_to_id.get(fid);
            dat_string.append(" f" + fid + " " + coflow.flows.get(flow_id).getDst_loc() + "\n");
        }
        dat_string.append(";\n\n");

        dat_string.append("param fv:=\n");
        for (int fid : flow_int_id_list) {
            String flow_id = flow_int_id_to_id.get(fid);
            dat_string.append(String.format(" f%d %.3f\n", fid, coflow.flows.get(flow_id).remaining_volume()));
        }
        dat_string.append(";\n\n");

        dat_string.append("end;\n");

        String dat_file_name = path_root + "/" + coflow.getId() + ".dat";

        try {
            PrintWriter writer = new PrintWriter(dat_file_name, "UTF-8");
            writer.println(dat_string.toString());
            writer.close();
        }
        catch (java.io.IOException e) {
//            System.out.println("ERROR: Failed to write to file " + dat_file_name);
            logger.error("ERROR: Failed to write to file {}" , dat_file_name);
            System.exit(1);
        }

        // Solve the LP
        String out_file_name = path_root + "/" + coflow.getId() + ".out";
        String command = "glpsol -m " + mod_file_name + " -d " + dat_file_name + " -o " + out_file_name;


        long LPTime = System.currentTimeMillis();
        try {
            Process p = Runtime.getRuntime().exec(command);
            p.waitFor();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        LPTime = System.currentTimeMillis() - LPTime;
        logger.info("LP time {} ms", LPTime);

        // Read the output
        MMCFOutput mmcf_out = new MMCFOutput();
        boolean missing_pieces = false;
        FileReader fr = new FileReader(out_file_name);
        BufferedReader br = new BufferedReader(fr);
        String line;
        String fs = "";
        String fe = "";
        int fi_int = -1;
        while ((line = br.readLine()) != null) {
            line = line.trim();

            // concatenate the lines if the first line has "[", and ends with "]"
            if (line.contains("[") && (line.substring(line.length() - 1).equals("]"))) {
                line = line + br.readLine();
            }

            if (line.contains("Objective")) {
                double alpha = Double.parseDouble(line.split("\\s+")[3]);
                if (alpha < 0.00001) {
                    logger.info("Optimizer: Coflow {} cannot be allocated on current network , alpha = {} ", coflow.getId() , alpha);
                    mmcf_out.completion_time_ = -1.0;
                    return mmcf_out;
                } else {
                    mmcf_out.completion_time_ = 1.0 / alpha;
                }
            } else if (line.contains("f[f") && !line.contains("NL")) {
                String[] splits = line.split("\\s+");
                String fsplits[] = splits[1].substring(3).split(",");
                fi_int = Integer.parseInt(fsplits[0]);
                fs = fsplits[1];
                fe = fsplits[2].split("]")[0];
                try {
                    // Quick hack to round to nearest 2 decimal places
                    double bw = Math.round(Double.parseDouble(splits[3]) * 100.0) / 100.0;
                    if (bw >= 0.01 && !fs.equals(fe)) {
                        if (mmcf_out.flow_link_bw_map_.get(fi_int) == null) {
                            mmcf_out.flow_link_bw_map_.put(fi_int, new ArrayList<>());
                        }
                        mmcf_out.flow_link_bw_map_.get(fi_int).add(new Link(fs, fe, bw));
                    }
                    missing_pieces = false;
                } catch (Exception e) {
                    missing_pieces = true;
                }
            } else if (!line.contains("f[f") && !line.contains("NL") && missing_pieces) {
                String[] splits = line.split("\\s+");
                try {
                    double bw = Math.round(Math.abs(Double.parseDouble(splits[1]) * 100.0) / 100.0);
                    if (bw >= 0.01 && !fs.equals(fe)) {
                        // At this point the flow id should be registered in the map
                        mmcf_out.flow_link_bw_map_.get(fi_int).add(new Link(fs, fe, bw));
                    }
                    missing_pieces = false;
                } catch (Exception e) {
                    missing_pieces = true;
                }
            } else if (line.contains("alpha")) {
                missing_pieces = false;
            }
        }
        br.close();
        return mmcf_out;
    }
}
