package gaiaframework.util;

// Create a configuration, either from config file, or from default value

//  # Format: (# for comment) // Comment line must start with # (blank not allowed)
//  MasterIP port
//  [num SAs]
//  ip port
//  [num RAs]
//  Ip port

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Configuration {

    protected String masterIP;
    protected int masterPort;

    int numDC;

    protected String configFilePath;

    class DataCenter {

        String saIP;
        String faIP;
        int saPort;
        int faPort;

        int numHosts;

        List<String> hostIPs;
        List<Integer> hostPorts;

    }

    List<DataCenter> dataCenters;

//    protected String[] SAIPs;
//    protected String[] RAIPs;
//
//    protected int[] SAPorts;
//
//    protected int[] RAPorts;

//    public Configuration(int numSA, int numRA) {
//        this.numRA = numRA;
//        this.numSA = numSA;
//        this.SAIPs = new String[numSA];
//        this.RAIPs = new String[numRA];
//        this.SAPorts = new int[numSA];
//        this.RAPorts = new int[numRA];
//        // default configuration
//        createDefaultConfig();
//    }

    public Configuration(int numDC, String configFile) {
        this.numDC = numDC;

        // the contents are initiated in parseConfigFile
        this.configFilePath = configFile;
        parseConfigFile(this.configFilePath);
        assert (numDC == this.numDC);
    }

    // sometimes we don't know how many RA/SAs (when invoked by Agents)
    public Configuration(String configFile) {
        this.configFilePath = configFile;
        parseConfigFile(this.configFilePath);
    }

    // create config according to default rules for maxAgents
    public Configuration(int maxAgents) {
        this.numDC = maxAgents;
        // default configuration
        createDefaultConfig();
    }

    public void createDefaultConfig() {

        System.err.println("no default setting allowed");
        System.exit(-1);
/*        for (int i = 0; i < numDC; i++) {
            SAIPs[i] = Constants.AGENT_ADDR_PREFIX + String.valueOf(i + 1); // starting from 10.0.0.1 !!!
            SAPorts[i] = Constants.SENDING_AGENT_PORT;
        }

        for (int i = 0; i < numDC; i++) {
            RAIPs[i] = Constants.AGENT_ADDR_PREFIX + String.valueOf(i + 1);
            RAPorts[i] = Constants.RECEIVING_AGENT_PORT;
        }

        masterPort = Constants.DEFAULT_MASTER_PORT;
        masterIP = Constants.AGENT_ADDR_PREFIX + String.valueOf(numDC + 1);*/

    }

    public boolean parseConfigFile(String configFilePath) {
        System.out.println("Loading config from " + configFilePath);
        try {
            FileReader fr = new FileReader(configFilePath);
            BufferedReader br = new BufferedReader(fr);
            String line;

            // First ignore comments at the beginning, comments are only allowed at the beginning for now.
            while ((line = br.readLine()).startsWith("#")) {
            }

            // next the first line

            String[] splits = line.split(" ");
            masterIP = splits[0];
            masterPort = Integer.parseInt(splits[1]);
            System.out.println("MS " + masterIP + ":" + masterPort);

            // second line
            line = br.readLine();
            numDC = Integer.parseInt(line);
            System.out.println("numDC = " + numDC);

            dataCenters = new ArrayList<>(numDC);

            // read the datacenters
            for (int i = 0; i < numDC; i++) {
                System.out.println("Reading from DC " + i);
                DataCenter dc = new DataCenter();

                // the first line
                line = br.readLine();

                splits = line.split(" ");
                int DCID = Integer.parseInt(splits[0]);
                assert (DCID == i);

                dc.numHosts = Integer.parseInt(splits[1]);

                // read SA
                line = br.readLine();
                splits = line.split(" ");

                dc.saIP = splits[0];
                dc.saPort = Integer.parseInt(splits[1]);
                System.out.println("SA " + dc.saIP + ":" + dc.saPort);

                // read FA
                line = br.readLine();
                splits = line.split(" ");

                dc.faIP = splits[0];
                dc.faPort = Integer.parseInt(splits[1]);
                System.out.println("FA " + dc.faIP + ":" + dc.faPort);

                dc.hostIPs = new ArrayList<>(dc.numHosts);
                dc.hostPorts = new ArrayList<>(dc.numHosts);


                // read hosts
                for (int j = 0; j < dc.numHosts; j++) {
                    line = br.readLine();
                    splits = line.split(" ");

                    dc.hostIPs.add(splits[0]);
                    dc.hostPorts.add(Integer.parseInt(splits[1]));
                    System.out.println("RA " + dc.hostIPs.get(j) + ":" + dc.hostPorts.get(j));

                }

                dataCenters.add(dc);
            }
            /*while ((line = br.readLine()) != null) {
                // Ignore comments
                if (line.charAt(0) == '#') {
                    continue;
                }

                switch (state) {
                    case 0:
                        // first line for master
                        String[] splits = line.split(" ");
                        masterIP = splits[0];
                        masterPort = Integer.parseInt(splits[1]);
                        System.out.println("MS " + masterIP + ":" + masterPort);

                        // state transition
                        state++;
                        break;

                    case 1:
                        // read numSA
                        numSA = Integer.parseInt(line);
                        System.out.println("numSA = " + numSA);
*//*                        if(numSA != Integer.parseInt(line)){
                            System.err.println("Configuration error!");
                            return false;
                        }*//*

                        if (numSA == 0) {
                            state += 2; // skip the next state
                        } else {
                            cnt = 0;
                            state++;
                        }
                        this.SAIPs = new String[numSA];
                        this.SAPorts = new int[numSA];
                        break;

                    case 2:
                        // read SAIPs
                        splits = line.split(" ");
                        SAIPs[cnt] = splits[0];
                        SAPorts[cnt] = Integer.parseInt(splits[1]);
                        System.out.println("SA " + cnt + ' ' + SAIPs[cnt] + ":" + SAPorts[cnt]);
                        cnt++;

                        if (cnt == numSA) {
                            cnt = 0;
                            state++;
                        }

                        break;

                    case 3:
                        // read numRA
                        numRA = Integer.parseInt(line);
                        System.out.println("numRA = " + numRA);
                        *//*if(numRA != Integer.parseInt(line)){
                            System.err.println("Configuration error!");
                            return false;
                        }*//*

                        if (numRA == 0) {
                            return true;
                        } else {
                            cnt = 0;
                            state++;
                        }
                        this.RAIPs = new String[numRA];
                        this.RAPorts = new int[numRA];
                        break;

                    case 4:
                        // read RAIPs
                        splits = line.split(" ");
                        RAIPs[cnt] = splits[0];
                        RAPorts[cnt] = Integer.parseInt(splits[1]);
                        System.out.println("RA " + cnt + ' ' + RAIPs[cnt] + ":" + RAPorts[cnt]);
                        cnt++;

                        if (cnt == numRA) {
                            return true;
                        }

                        break;

                }
            }*/
        } catch (FileNotFoundException e) {
            System.err.println("Config file path invalid, fall back to default config.");
            createDefaultConfig();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }

    public String getSAIP(int i) {
        assert (i >= 0 && i < numDC);
        return dataCenters.get(i).saIP;
//        return SAIPs[i];
    }

    public int getNumDC() {
        return numDC;
    }

    public List<String> getHostIPbyDCID(int i) {
        return dataCenters.get(i).hostIPs;
    }

    public List<Integer> getHostPortbyDCID (int i) {
        return dataCenters.get(i).hostPorts;
    }

    public int getNumHostbyDCID (int i) {
        return dataCenters.get(i).numHosts;
    }

    public int getSAPort(int i) {
        assert (i >= 0 && i < numDC);
        return dataCenters.get(i).saPort;
//        return SAPorts[i];
    }

    public String getFAIP(int i) {
        assert (i >= 0 && i < numDC);
        return dataCenters.get(i).faIP;
//        return RAIPs[i];
    }

    public int getFAPort(int i) {
        assert (i >= 0 && i < numDC);
        return dataCenters.get(i).faPort;
//        return RAPorts[i];
    }

    public int getMasterPort() {
        return masterPort;
    }

    public String getMasterIP() {
        return masterIP;
    }

    //
    public String findDCIDbyHostAddr(String addr) {

        // version 2, we only compare IP
        for (int i = 0; i < numDC; i++) {
            List<String> raIPs = dataCenters.get(i).hostIPs;
            for (int j = 0; j < dataCenters.get(i).numHosts; j++) {

                if ((raIPs.get(j)).equals(addr.split(":")[0])) {
                    return String.valueOf(i);
                }
            }
        }


/*
        for (int i = 0; i < numDC; i++) {
            List<String> raIPs = dataCenters.get(i).hostIPs;
            List<Integer> raPorts = dataCenters.get(i).hostPorts;
            for (int j = 0; j < dataCenters.get(i).numHosts; j++) {

                if ((raIPs.get(j) + ":" + raPorts.get(j)).equals(addr)) {
                    return String.valueOf(i);
                }
            }
        }
*/

        return null;
    }

/*    public String findSrcIDbyAddr(String MapAddr) {

        for (int i = 0; i < numDC; i++) {
            List<String> srcIPs = dataCenters.get(i).hostIPs;
            List<Integer> srcPorts = dataCenters.get(i).hostPorts;
            for (int j = 0; j < dataCenters.get(i).numHosts; j++) {

                if ((IPs.get(j) + ":" + raPorts.get(j)).equals(reducerAddr)) {
                    return String.valueOf(i);
                }
            }
        }

        return null;
    }*/

/*    public String findFAIDbyIP(String reducerIP) {

        for (int i = 0; i < numDC; i++) {
            if (dataCenters.get(i).faIP.equals(reducerIP)) {
                return String.valueOf(i);
            }
        }

        return null;
    }

    public String findSAIDbyIP(String mapperIP) {
        for (int i = 0; i < numDC; i++) {
            if (dataCenters.get(i).saIP.equals(mapperIP)) {
                return String.valueOf(i);
            }
        }

        return null;
    }*/
}
