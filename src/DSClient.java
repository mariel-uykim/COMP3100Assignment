/**
 * This is a Client-side simulator for a distributed system job scheduler.
 * This is written in Java and needs the DS-Server prorgam in order to successfully
 * work. 
 * 
 * Created by: Mariel Anne Uykim (46129448) 
 * For: COMP3100 Assignment 1 
 */

import java.net.*;
import java.io.*;
import java.util.*;

public class DSClient {

    private static Socket s;
    private static BufferedReader input;
    private static DataOutputStream output;
    private static String username = System.getProperty("user.name");

    /**
    * serverConn(): This method connects to the server-side 
    * simulator and returns true if successful and false if 
    * it failed to connect
    *
    * @param  hostid  the IP address of the server
    * @param  port the port the server is listening in
    * @return connection success/failure
    */
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

    /**
    * initialise(): This method starts the communication with
    * the server-side simulator and authenticates the current 
    * user by getting the user name of the system. Returns true
    * if successfully authenticated and false if failed to do so.
    *
    * @return authentication success/failure
    */
    public static Boolean initialise() throws IOException{
        sendMsg("HELO");

        if(printRes(true).equals("OK")) {
            sendMsg("AUTH " + username);
        }

        if(printRes(false).equals("OK")){
            return true;
        }
        return false;
    }

    /**
    * printRes(): This method reads the response from the server 
    * through BufferReader's readLine() method. It prints the 
    * response if print is true. The method returns the response.
    *
    * @param  print if it should print out the response in the console
    * @return response
    */
    public static String printRes(Boolean print) throws IOException {
        String response = "";
        response = input.readLine();

        if(print) {
            System.out.println("SERVER: " + response);
        }
        return response;
    }

    /**
    * sendMsg(): This method sends a message to the server by using
    * DataOutputStream's write() method.
    *
    * @param  msg message to be sent
    */
    public static void sendMsg(String msg) throws IOException {
        output.write((msg+"\n").getBytes());
        output.flush();
    }

    /**
    * closeConn(): This method closes all connections to the server
    * and prints out an error if connection problem occurs
    */
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

    /**
    * getInfo(): This method gets server data from the server-side simulator
    * using the GETS command. Depending on the option the user selects, the
    * information retrieved varies. Using option 1 gets servers with specific
    * capabilities for the job while selecting option 2 retrieves all existing 
    * servers.
    *
    * @param  response  the JOBN response of the server
    * @param  option the type of information to get
    * @return number of servers available or -1 if none
    */
    public static int getInfo(String response, int option) throws IOException {
        if(option == 1) {
            //split response string into several parts
            String [] res = response.split(" ", 7);
            String send = "GETS Capable ";

            //combine last three digits
            for(int i = (res.length)-3; i < res.length; i++){
                send += (res[i] + " ");
            }

            sendMsg(send);
        }
        else if(option == 2) {
            sendMsg("GETS All");
        }
        String data = printRes(false);
        
        //split data if not error
        if(data.contains("DATA")){
            String [] dataRec = data.split(" ", 3);

            String nData = dataRec[1];

            if(nData == "."){
                return -1;
            }
            return Integer.parseInt(nData);
        }
        return -1;
    }

    /**
    * getServers(): This method reads all the server data sent by the
    * server-side simulator based on the number of servers available by
    * using a for loop and stores it into a string.
    *
    * @param  nData  number of server data to read
    * @return a string of all server data separated by "###"
    */
    public static String getAllServers(int nData) throws IOException {
        //stores response on server information
        String reply = null;
        String serverInfo = "";

        //concatenate each new server to serverInfo
        for(int i = 0; i < nData; i++) {
            reply = input.readLine();

            if(i == nData-1) {
                serverInfo += (reply);
            }
            
            else {
                serverInfo += (reply + "###");
            }
        }

        sendMsg("OK");

        return serverInfo;

    }

    /**
    * getLargestServer(): This method calls getAllServers to get server data
    * and splits this into a 2D array. While storing into the array, it checks
    * for the number of cores from each server and compares them to that on the
    * 2D ArrayList which stores the highest count so far. It then returns this
    * ArrayList.
    *
    * @param  nData  number of server data to read
    * @return 2D ArrayList of servers with the largest server types
    */
    public static ArrayList<ArrayList<String>> getLargestServer(int nData) throws IOException {
        String data = getAllServers(nData);
        int nDataFields = 9;
        int coreIndex = 4;
        int typeIndex = 0;
        int lsIdx = 0;

        String [] list = data.split("###");
        ArrayList<ArrayList<String>> largestServer = new ArrayList<ArrayList<String>>();
        String [][] servers = new String[list.length][nDataFields];

        for(int i = 0; i < list.length; i++) {
            String [] temp = list[i].split(" ");

            if(temp.length == nDataFields) {
                for(int j = 0; j < nDataFields; j++) {  
                    servers[i][j] = temp[j];
                }
            }
            
            //add to list if the same size
            if(largestServer.isEmpty() || (
               servers[i][coreIndex].equals(largestServer.get(0).get(coreIndex)) &&
               servers[i][typeIndex].equals(largestServer.get(0).get(typeIndex)))) {

                largestServer.add(new ArrayList<String>());

                for(int j = 0; j < nDataFields; j++) {
                    
                    largestServer.get(lsIdx).add(servers[i][j]);

                }

                lsIdx++;
            }

            //restart list if greater
            else if(Integer.parseInt(servers[i][coreIndex]) >
                    Integer.parseInt(largestServer.get(0).get(coreIndex))) {

                lsIdx = 0;
                
                //clear current list
                largestServer.clear();
                largestServer.add(new ArrayList<String>());

                for(int j = 0; j < nDataFields; j++) {
                    
                    largestServer.get(lsIdx).add(servers[i][j]);

                }


                lsIdx++;
            }
        }
	
        return largestServer;
    }

    /**
    * getJobID(): This method gets the JOBN response of the server-side 
    * simulator and splits it to get the job id. 
    *
    * @param  response  the JOBN response of the server
    * @return job id of response
    */
    public static int getJobID(String response) {
        String [] data = response.split(" ");
        return Integer.parseInt(data[2]);
    }

    /**
    * DSClient(): This is the constructor for the DSClient class.
    * This connects to the server-side simulator, gets server information,
    * and schedules job using the largest round robin algorithm.
    *
    * @param  hostid  the ip address of the server
    * @param port the port that server is listening to
    */
    public DSClient(String hostid, int port) {
	    
        //try to connect to server
        Boolean result = serverConn(hostid, port);

        //exit if failed to connect
        if(result == false) {

            System.out.println("Failed to conencect..");
            return;
        }

        try {

            //starts connection and authenticates with server
            if(!initialise()) {
                return;
            }

            //tells server 'redy' to start sending commands
            sendMsg("REDY");

            //stores server response in jobn variable
            String jobn = printRes(false);

            //get list of servers
            int nData = getInfo(jobn, 2);

            //quit if no servers or an error happens
            if(nData == -1) {
                sendMsg("QUIT");
            }
            
            //acknowleges server repsonse
            sendMsg("OK");

            //gets largest server and stores in array
            ArrayList<ArrayList<String>> servers = getLargestServer(nData);

            //clear input stream
            printRes(false);

            int i = 0;
            while(true) {
                if(i >= servers.size()) {
                    i = 0;
                }

                String serverType = servers.get(i).get(0);
                String serverID = servers.get(i).get(1);

                //skips current iteration when server sends JCPL data
                if(jobn.contains("JCPL")) {
                    sendMsg("REDY");
                    jobn = printRes(false);
                    if(jobn.contains("NONE")) {
                        sendMsg("QUIT");
                        break;
                    }
                    continue;
                }

                //sends a schedule to server consisting of current job id, the server type,
                //and server id
                String sched = "SCHD " + getJobID(jobn) + " " + serverType + " " + serverID;
                sendMsg(sched);

                i++;

                printRes(false);

                sendMsg("REDY");

                jobn = printRes(false);

                //end loop when no more jobs
                if(jobn.contains("NONE")) {
                    sendMsg("QUIT");
                    break;
                }


            }
            
        }
        catch(Exception e) {
            System.out.println(e);
        }

        //ends connection
        closeConn();
    }

    public static void main(String args[]) {
        DSClient c = new DSClient("127.0.0.1",50000);
    }
}


