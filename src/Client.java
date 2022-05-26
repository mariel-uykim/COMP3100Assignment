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
    private static HashMap<String, ArrayList<Integer>> runningJobs = new HashMap<String, ArrayList<Integer>>();
    private static int jobCount = 0;

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

    //initialise(): initialises and authenticates with server
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

    //response(): returns response of servers and prints out response
    //if print is true.
    public static String response(Boolean print) throws IOException {
        String response = "";
        response = input.readLine();

        if(print) {
            System.out.println("SERVER: " + response);
        }
        return response;
    }

    //send(): Sends message to server
    public static void send(String msg) throws IOException {
        output.write((msg+"\n").getBytes());
        output.flush();
    }

    //getServerInfo(): from the job info statements, it gets the available
    //resources available 
    public static int getServerInfo(String response, int option) throws IOException {

        String [] res = response.split(" ", 7);
        String message = "GETS Capable ";

        if(option == 2) {
            message = "GETS Avail ";
        }

        for(int i = (res.length)-3; i < res.length; i++){
            message += (res[i] + " ");
        }

        send(message);

        String data = response(false);

        if(data.contains("DATA")){
            String [] dataRec = data.split(" ", 3);
            
            int nRecIdx = 1;
            String nData = dataRec[nRecIdx];

            if(nData == "." || Integer.parseInt(nData) <= 0){
                send("OK");
                response(false);
                return -1;
            }
            return Integer.parseInt(nData);
        }

        return -1;

    }

    //closeConn(): closes the connection
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
    
    //getPendingJobs(): sends the LSTJ command to the server and returns the
    ///first waiting job
    public static String [] getPendingJobs(String sType, String sID) throws IOException {

        send("LSTJ " + sType + " " + sID);

        String [] pending = null;
        int nDataFields = 9;
        String resp = response(false);
        int nResp = 8;

        if(resp.contains("DATA")) {
            String [] data = resp.split(" ");
            int nData = Integer.parseInt(data[1]);

            for(int i = 0; i < nData; i++) {
                String jobQueued = response(false);
                String [] temp = jobQueued.split(" ");
                if(Integer.parseInt(temp[3]) == -1 && i == nData-1) {

                    pending = new String[nResp];

                    for(int j = 0; j < nResp; j++) {
                        pending[j] = temp[j];
                    }
                }
            }

            send("OK");
            response(false);
        }

        return pending;

    }
    
    //getAllServers(): gets all servers sent by DS server and returns a 2D array containing
    //this information
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

    //getBestServer(): Best server uses a fitness value and other criterias to 
    //determine which server to use.
    public static String[] getBestServer(int nData, int jobCore) throws IOException {

        //index of server information in allServers array
        int typeIndex = 0;
        int idIndex = 1;
        int stateIndex = 2;
        int sTimeIndex = 3;
        int coreIndex = 4;
        int wJobIndex = 7;
        int rJobIndex = 8;
        int nDataFields = 9;
        
        String [] bestServer = null;
        ArrayList <String> serverTypes = new ArrayList<String>();

        String [][] allServers = new String[nData][nDataFields];
        ArrayList <Integer> fVals = new ArrayList<Integer>();

        allServers = getAllServers(nData, nDataFields);

        //get fitness values of all servers and get all server types in order
        for(int i = 0; i < allServers.length; i++) {
            //fitness value = available core of server - cores required for job
            int fv = Integer.parseInt(allServers[i][coreIndex]) - jobCore;
            fVals.add(fv);

            //adds server type to server list if it is not in it yet
            if(!serverTypes.contains(allServers[i][typeIndex])) {
                serverTypes.add(allServers[i][typeIndex]);
            }
        }

        //sorts fVals from lowest to highest
        ArrayList <Integer> sortedfVals = new ArrayList<Integer>(fVals);
        // sortedfVals.sort(Comparator.reverseOrder());
        Collections.sort(sortedfVals);

        for(Integer i : sortedfVals) {

            //gets the index of the array that has 
            int curr = fVals.indexOf(i);
            int wJobs = Integer.parseInt(allServers[curr][wJobIndex]);
            int rJobs = Integer.parseInt(allServers[curr][rJobIndex]);

            if((wJobs == 0 || rJobs == 0) && i >= 0) {
                bestServer = new String[nDataFields];
                for(int j = 0; j < nDataFields; j++) {                    
                    bestServer[j] = allServers[curr][j];
                }
                break;
                
            }
        }

        if(bestServer == null) {
            boolean isActive = false;
            int k = 0;

            while(!isActive && k < allServers.length) {
                
                bestServer = new String[nDataFields];

                if(allServers[k][stateIndex].equals("active") ||
                   allServers[k][stateIndex].equals("booting") ||
                   allServers[k][stateIndex].equals("idle")) {
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

    //getJobInfo(): outputs information of job depending on what
    //data is given. Option 'c' gives job cores while option 'i'
    //gives job id from statement
    public static int getJobInfo(String jobn, char option) {
        int idx = 0;

        if(option == 'c') {
            idx = 4;
        }
        if(option == 'i') {
            idx = 2;
        }
        String [] data = jobn.split(" ");
        if(data[idx] == null) {
            return -1;
        }
        return Integer.parseInt(data[idx]);
    }

    public static void migrateServer(int jID, String srcType, String srcID, String tgtType, String tgtID) throws IOException {
        String msg = "MIGJ " + jID + " " + srcType + " " + srcID + " " + tgtType + " " + tgtID;
        send(msg);
        modifyRunningJobs(tgtType, tgtID, jID, 2);
        response(false);
    }

    //getNextBest: find alternative vacant server
    public static void getNextBest(String sType, String sID) throws IOException {
        String [] pendingJob = getPendingJobs(sType, sID);

        if(pendingJob != null) {
            String jobInfo = "job time id run " + pendingJob[5] + " " + pendingJob[6] + " " + pendingJob[7];
            int nData = getServerInfo(jobInfo, 2);
            int nDataFields = 9;
           
            String [][] allServers = getAllServers(nData, nDataFields);

            for(int i = 0; i < allServers.length; i++) {
                int fV = Integer.parseInt(allServers[i][4]) - Integer.parseInt(pendingJob[5]);
                if(fV >= 0 && fV <= 5 && (allServers[i][2].equals("active") || allServers[i][2].equals("idle"))) {
                    migrateServer(Integer.parseInt(pendingJob[0]), sType, sID, allServers[i][0], allServers[i][1]);
                    break;
                }
            }
        }
    }

    //findCrowdedServer(): return server with the highest load
    public static String [] findCrowdedServer() throws IOException {
        String mostCrowded = new String();
        int nJobs = 0;
        String [] server = new String[3];

        for(String i : runningJobs.keySet()) {
            if(mostCrowded.length() == 0) {
                mostCrowded = i;
                nJobs = runningJobs.get(i).size();
            }
            else if (runningJobs.get(i).size() > 
                    runningJobs.get(mostCrowded).size()) {
                mostCrowded = i;
                nJobs = runningJobs.get(i).size();
            }
        }

        String temp [] = mostCrowded.split("||");

        server[0] = temp[0];
        server[1] = temp[1];
        server[2] = nJobs+"";

        return server;

    }
    
    //modifyRunningJobs(): modifies global variable 'runningJobs' that keeps 
    //track of all job acitivities. Option 0->delete job, Option 1-> add job, Option 2-> change server. 
    public static void modifyRunningJobs(String sType, String sId, int jobId, int option) {

        if(option == 1){
            String serverInfo = sType + "||" + sId;
            ArrayList <Integer> tempArr = runningJobs.get(serverInfo);
            if(tempArr == null) {
                tempArr = new ArrayList<Integer>();
            }
            tempArr.add(jobId);
            runningJobs.put(serverInfo, tempArr);
            
        }
        else if (option == 2){
            modifyRunningJobs(sType, sId, jobId, 0);
            modifyRunningJobs(sType, sId, jobId, 1);
        }
        else {
           for (String j : runningJobs.keySet()) {
               if(runningJobs.get(j).contains(jobId)) {
                    ArrayList <Integer> tempArr = runningJobs.get(j);
                    tempArr.remove(tempArr.indexOf(jobId));
                    runningJobs.put(j, tempArr);
                    break;
               }
            }
        }
    }

    //check pending jobs of other servers and migrate others to empty servers
    //add on top of fv check other performance matrix

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

                //skip current iteration if sent response is JCPL 
                if(currJob.contains("JCPL")) {
                    modifyRunningJobs("", "", getJobInfo(currJob, 'i'), 0);
                    jobCount--;
                    continue;
                }

                //quit if no more jobs remaining
                if(currJob.contains("NONE")) {
                    send("QUIT");
                    break;
                }

                int nData = getServerInfo(currJob, 2);

                if(nData == -1) {
                    nData = getServerInfo(currJob, 1);

                    if(nData == -1) {
                        send("QUIT");
                        break;
                    }
                }
                
                send("OK");

                String [] server = getBestServer(nData, getJobInfo(currJob, 'c'));

                String serverType = server[0];
                String serverID = server[1];
                int jobID = getJobInfo(currJob, 'i');

                response(false);

                String sched = "SCHD " + jobID + " " + serverType + " " + serverID;
                send(sched);

                modifyRunningJobs(serverType, serverID, jobID, 1);
                jobCount++;

                response(false);

                if(jobCount > 10) {
                    String [] crowdedServer = findCrowdedServer();
                    if(Integer.parseInt(crowdedServer[2]) > 5) {
                        getNextBest(crowdedServer[0], crowdedServer[1]);
                    }
                }
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

