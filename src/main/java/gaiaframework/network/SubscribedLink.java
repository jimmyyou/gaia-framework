package gaiaframework.network;

import java.util.ArrayList;

// A SubscribedLink has a number of flows currently using it
public class SubscribedLink extends Link {

    // Flows which are currently using this link
    public ArrayList<Pathway> subscribers_ = new ArrayList<Pathway>();

    public SubscribedLink(double max_bw) {
        super(max_bw);
    }

    // Return the amount of bandwidth allocated to each subscribing
    // flow if all subscribers receive an equal amount.
    public double bw_per_flow() {
        return subscribers_.isEmpty() ? max_bw_ : max_bw_ / (double)subscribers_.size();
    }

    // Return the amount of bandwidth not yet allocated.
    public double remaining_bw() {
        double remaining_bw = max_bw_;
        for (Pathway p : subscribers_) {
            remaining_bw -= p.getBandwidth();
        }

        return remaining_bw;
    }
}
