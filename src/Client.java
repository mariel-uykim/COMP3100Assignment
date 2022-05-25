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
            message = "GETS Avail";
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

            if(nData == "." || nData.length() <= 0){
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
    
    //getPendingJobs(): sends the LSTJ job to the server and returns the
    ///number of waiting jobs
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

            if((wJobs == 0 || rJobs == 0) &&
                wJobs < 2 && 
                && i >= 0) {
                if(bestServer == null) {
                    bestServer = new String[nDataFields];
                }
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

    //modifyRunningJobs(): modifies global variable 'runningJobs' that keeps 
    //track of all job acitivities. Option 0->delete job, Option 1-> add job. 
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
    //checkServerJobs(): check current jobs in server, returns percentage of no. of running jobs 
    private static float checkServerJobs(String type, String id) {
        String serverInfo = type + "||" + id;
        ArrayList<Integer> tempArr = runningJobs.get(serverInfo);
        float percent = 0;

        if(tempArr != null && jobCount > 0) {
            percent = tempArr.size()/jobCount;
        }

        return percent;
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

