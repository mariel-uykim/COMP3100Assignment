import java.net.*;
import java.io.*;
import java.util.*;
import java.util.ArrayList;

import javax.swing.plaf.TreeUI;

public class DSClient {

    private static Socket s;
    private static BufferedReader input;
    private static DataOutputStream output;
    private static String username = System.getProperty("user.name");

    public static boolean serverConn(String hostid, int port) {
        try {
            s = new Socket(hostid, port);
            System.out.println("Connection Success!");

            input = new BufferedReader(new InputStreamReader(s.getInputStream()));
            output = new DataOutputStream(s.getOutputStream());

            return true;
            
        }
        catch(Exception e) {
            System.out.println(e);
        }

        return false;
    }

    public static Boolean initialise() throws IOException{
        send("HELO");

        if(response(false).equals("OK")) {
            send("AUTH " + username);
        }

        if(response(false).equals("OK")){
            return true;
        }
        return false;
    }

    public static String response(Boolean print) throws IOException {
        String response = "";
        response = input.readLine();

        if(print) {
            System.out.println("SERVER: " + response);
        }
        return response;
    }

    public static void send(String msg) throws IOException {
        output.write((msg+"\n").getBytes());
        output.flush();
    }

    public static int getServerInfo(String response) throws IOException {

        String [] res = response.split(" ", 7);
        String message = "GETS Capable ";

        for(int i = (res.length)-3; i < res.length; i++){
            message += (res[i] + " ");
        }

        send(message);

        String data = response(false);

        if(data.contains("DATA")){
            String [] dataRec = data.split(" ", 3);
            
            int nRecIdx = 1;
            String nData = dataRec[nRecIdx];

            if(nData == "." || nData.length() <= 0){
                return -1;
            }
            return Integer.parseInt(nData);
        }

        return -1;

    }
    public static void closeConn() {
        try {
            System.out.println("terminating");
            input.close();
            output.close();
            s.close();
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
    
    public static int getPendingJobs(String server) throws IOException {
        int total = 0;

        send("LSTJ " + server);

        String resp = response(false);

        if(resp.contains("DATA")) {
            String [] data = resp.split(" ");
            int nData = Integer.parseInt(data[1]);
            int counter = 0;

            for(int i = 0; i < nData; i++) {
                response(false);
                counter++;
            }

            total = counter;
            send("OK");
            response(false);
        }

        return total;

    }
    
    //gets all servers sent by DS server
    public static String[][] getAllServers (int nData, int nDataFields) throws IOException {
        String [][] allServers = new String[nData][nDataFields];

        for(int i = 0; i < nData; i++) {
            String [] reply = response(false).split(" ");

            for(int j = 0; j < nDataFields;j++) {
                allServers[i][j] = reply[j];
            }
            
        }

        return allServers;
    }

    //best server uses a fitness value and other criterias to determine which server
    //to use.
    public static String[] getBestServer(int nData, int jobCore) throws IOException {
        int nDataFields = 9;
        int coreIndex = 4;
        int typeIndex = 0;
        int wJobIndex = 7;
        int rJobIndex = 8;
        int sTimeIndex = 3;
        int idIndex = 1;
        int stateIndex = 2;

        String [] bestServer = null;
        ArrayList <String> serverTypes = new ArrayList<String>();

        String [][] allServers = new String[nData][nDataFields];
        ArrayList <Integer> fVals = new ArrayList<Integer>();

        allServers = getAllServers(nData, nDataFields);

        //get fVals of all and get all server types
        for(int i = 0; i < allServers.length; i++) {
            int fv = Integer.parseInt(allServers[i][coreIndex]) - jobCore;
            fVals.add(fv);

            if(!serverTypes.contains(allServers[i][typeIndex])) {
                serverTypes.add(allServers[i][typeIndex]);
            }
        }

        ArrayList <Integer> sortedfVals = new ArrayList<Integer>(fVals);
        sortedfVals.sort(Comparator.reverseOrder());

        for(Integer i : sortedfVals) {
            int curr = fVals.indexOf(i);
            int wJobs = Integer.parseInt(allServers[curr][wJobIndex]);
            int rJobs = Integer.parseInt(allServers[curr][rJobIndex]);

            if((wJobs == 0 || rJobs == 0) && i >= 0) {
                if(bestServer == null) {
                    bestServer = new String[nDataFields];
                    
                }
                for(int j = 0; j < nDataFields; j++) {                    
                    bestServer[j] = allServers[curr][j];
                }
            }
        }

        if(bestServer == null) {
            boolean isActive = false;
            int k = 0;

            while(!isActive && k < allServers.length) {
                
                bestServer = new String[nDataFields];

                if(allServers[k][stateIndex].equals("Active") ||
                   allServers[k][stateIndex].equals("Booting") ||
                   allServers[k][stateIndex].equals("Idle")) {
                    for(int j = 0; j < nDataFields; j++) {
                        bestServer[j] = allServers[k][j];
                    }
                    isActive = true;
                }

                k++; 
            }
            if(isActive == false) {
                for(int j = 0; j < nDataFields; j++) {
                    bestServer[j] = allServers[0][j];
                }
            }
        }

        send("OK");
      
        return bestServer;
    }

    public static int getJobInfo(String jobn, char option) {
        int idx = 0;

        if(option == 'c') {
            idx = 4;
        }
        if(option == 'i') {
            idx = 2;
        }
        String [] data = jobn.split(" ");
        return Integer.parseInt(data[idx]);
    }

    public DSClient(String hostid, int port) {
        //try to connect to server
        Boolean result = serverConn(hostid, port);

        if(result == false) {
            System.out.println("Failed to conencect..");
            return;
        }
        try {
            if(!initialise()) {
                return;
            }
            while(true) {

                send("REDY");
                
                String currJob = response(false);

                if(currJob.equals(".")) {
                    currJob = response(false);
                }

                if(currJob.contains("JCPL")) {
                    continue;
                }

                if(currJob.contains("NONE")) {
                    send("QUIT");
                    break;
                }

                int nData = getServerInfo(currJob);

                if(nData == -1) {
                    send("QUIT");
                    break;
                }
                
                send("OK");
                String [] server = getBestServer(nData, getJobInfo(currJob, 'c'));

                String serverType = server[0];
                String serverID = server[1];

                response(false);
                String sched = "SCHD " + getJobInfo(currJob, 'i') + " " + serverType + " " + serverID;
                send(sched);

                response(false);
            }
        }

        catch(Exception e) {
            System.out.println(e);
        }
        
        closeConn();
    }

    public static void main(String args[]) {
        DSClient c = new DSClient("127.0.0.1",50000);
    }
}

