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

public class Configuration {

    protected int masterPort;
    protected String masterIP;

    protected int numSA;
    protected int numRA;
    protected String configFilePath;

    protected String[] SAIPs;
    protected String[] RAIPs;

    protected int[] SAPorts;

    protected int[] RAPorts;

    public Configuration(int numSA, int numRA) {
        this.numRA = numRA;
        this.numSA = numSA;
        this.SAIPs = new String[numSA];
        this.RAIPs = new String[numRA];
        this.SAPorts = new int[numSA];
        this.RAPorts = new int[numRA];
        // default configuration
        createDefaultConfig();
    }

    public Configuration(int numSA, int numRA, String configFile) {
        this.numRA = numRA;
        this.numSA = numSA;

        // the arrays are initiated in parseConfigFile

        this.configFilePath = configFile;
        parseConfigFile(this.configFilePath);
    }

    // sometimes we don't know how many RA/SAs (when invoked by Agents)
    public Configuration(String configFile) {
        this.configFilePath = configFile;
        parseConfigFile(this.configFilePath);
    }

    // create config according to default rules for maxAgents
    public Configuration(int maxAgents) {
        this.numSA = maxAgents;
        this.numRA = maxAgents;
        this.SAIPs = new String[numSA];
        this.RAIPs = new String[numRA];
        this.SAPorts = new int[numSA];
        this.RAPorts = new int[numRA];
        // default configuration
        createDefaultConfig();
    }

    public void createDefaultConfig() {
        for (int i = 0; i < numSA; i++) {
            SAIPs[i] = Constants.AGENT_ADDR_PREFIX + String.valueOf(i + 1); // starting from 10.0.0.1 !!!
            SAPorts[i] = Constants.SENDING_AGENT_PORT;
        }

        for (int i = 0; i < numRA; i++) {
            RAIPs[i] = Constants.AGENT_ADDR_PREFIX + String.valueOf(i + 1);
            RAPorts[i] = Constants.RECEIVING_AGENT_PORT;
        }

        masterPort = Constants.DEFAULT_MASTER_PORT;
        masterIP = Constants.AGENT_ADDR_PREFIX + String.valueOf(numRA + 1);

    }

    public boolean parseConfigFile(String configFilePath) {
        System.out.println("Loading config from " + configFilePath);
        try {
            FileReader fr = new FileReader(configFilePath);
            BufferedReader br = new BufferedReader(fr);

            int cnt = 0;
            String line;
            int state = 0; // 0 - reading master; 1 - #SA; 2 - SAIPs; 3 - #RA; 4 - RAIPs
            while ((line = br.readLine()) != null) {
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
/*                        if(numSA != Integer.parseInt(line)){
                            System.err.println("Configuration error!");
                            return false;
                        }*/

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
                        /*if(numRA != Integer.parseInt(line)){
                            System.err.println("Configuration error!");
                            return false;
                        }*/

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
            }
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
        assert (i >= 0 && i < numSA);
        return SAIPs[i];
    }

    public int getSAPort(int i) {
        assert (i >= 0 && i < numSA);
        return SAPorts[i];
    }

    public String getRAIP(int i) {
        assert (i >= 0 && i < numRA);
        return RAIPs[i];
    }

    public int getRAPort(int i) {
        assert (i >= 0 && i < numRA);
        return RAPorts[i];
    }

    public int getMasterPort() {
        return masterPort;
    }

    public String getMasterIP() {
        return masterIP;
    }

    public int getNumSA() {
        return numSA;
    }

    public int getNumRA() {
        return numRA;
    }

    public String findRAIDbyIP(String reducerIP) {

        for (int i = 0; i < RAIPs.length; i++) {
            if (RAIPs[i].equals(reducerIP)){
                return String.valueOf(i);
            }
        }

        return null;
    }

    public String findSAIDbyIP(String mapperIP) {
        for (int i = 0; i < SAIPs.length; i++) {
            if (SAIPs[i].equals(mapperIP)){
                return String.valueOf(i);
            }
        }

        return null;
    }
}
