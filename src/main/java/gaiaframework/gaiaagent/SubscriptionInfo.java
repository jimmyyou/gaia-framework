package gaiaframework.gaiaagent;

// Stores subscription information fgid -> FGI, rate
// So we need a collection of SubscriptionInfo to store all subscriptions.

public class SubscriptionInfo {
    final String fgid;
    final AggFlowGroupInfo fgi;
    volatile double rate;

    public SubscriptionInfo(String id, AggFlowGroupInfo fgi, double rate) {
        this.fgid = id;
        this.fgi = fgi;
        this.rate = rate;
    }

    public String getId() {
        return fgid;
    }

    public AggFlowGroupInfo getFgi() {
        return fgi;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double newRate) { this.rate = newRate; }
}
