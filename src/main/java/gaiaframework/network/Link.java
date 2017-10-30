package gaiaframework.network;

import java.util.ArrayList;

import gaiaframework.util.Constants;

public class Link {
    // The maximum bandwidth this link can serve
    public double max_bw_;
    public double cur_bw_;
    public String src_loc_;
    public String dst_loc_;

    private double previous_bw;

    public Link(double max_bw) {
        max_bw_ = max_bw;
        cur_bw_ = max_bw;
    }

    public Link(String src_loc, String dst_loc, double max_bw) {
        src_loc_ = src_loc;
        dst_loc_ = dst_loc;
        max_bw_ = max_bw;
        cur_bw_ = max_bw;
    }

    public String toString() {
        return "[ " + Constants.node_id_to_trace_id.get(src_loc_) + ", " + Constants.node_id_to_trace_id.get(dst_loc_) + "] " + cur_bw_;
    }

    public double getCur_bw_() { return cur_bw_; }

    public void setCur_bw_(double cur_bw_) { this.cur_bw_ = cur_bw_; }

    public double getMax_bw_() { return max_bw_;}

    public void setMax_bw_(double max_bw_) { this.max_bw_ = max_bw_; }

    public void goDown() {
        previous_bw = max_bw_;
        max_bw_ = 0;
        cur_bw_ = 0;
    }

    public void goUp() {
        max_bw_ = previous_bw;
        cur_bw_ = previous_bw;
    }
}
