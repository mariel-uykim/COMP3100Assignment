import java.net.*;
import java.io.*;
import java.util.Arrays;

public class DSClient {
    private static Socket s;
    private static BufferedReader input;
    private static DataOutputStream output;
    private static String username = System.getProperty("user.name");

    //serverConn(): estalishes connection with the server
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

    //initialise(): starts communication and authentication with server
    public static Boolean initialise() throws IOException{
        sendMsg("HELO");

        if(printRes().equals("OK")) {
            sendMsg("AUTH " + username + "\n");
        }

        if(printRes().equals("OK")){
            return true;
        }
        return false;
    }

    //printRes(): gets and prints out received message
    public static String printRes() throws IOException {
        String response = "";
        response = input.readLine();

        System.out.println("SERVER: " + response);
        return response;
    }

    //sendMsg(): sends message to server
    public static void sendMsg(String msg) throws IOException {
        output.write((msg+"\n").getBytes());
        output.flush();
    }

    //closeConn(): close connection
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

    //getInfo()): get data from jobn response if 1 or all if 0, 
    //returns number of data
    public static int getInfo(String response, int option) throws IOException{
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
        String data = printRes();
        
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

    //getLargestServer(): gets Largest server (last on list)
    public static String [] getLargestServer(String data) {
        String [] list = data.split("--");
        String largest = list[list.length-1];
        String [] serverInfo = largest.split(" "); 
        return serverInfo;
    }

    //schedule(): schedules a job to DSServer. Returns false if
    //failed
    public static Boolean schedule(int jobID, String sType, int sID) throws IOException {
        sendMsg("SCHD " + jobID + " " + sType + " " + sID);
        return true;
    }

    //getJobID(): gets jobID from server response
    public static int getJobID(String response) {
        String [] data = response.split(" ");
        return Integer.parseInt(data[2]);
    }
    //DSClient(): Class instantiates server connection and communicates
    //with DS Server 
    public DSClient(String hostid, int port) {
	    
        //try to connect to server
        Boolean result = serverConn(hostid, port);

        if(result == false) {
            return;
        }

        try {
            if(!initialise()) {
                return;
            }

            sendMsg("REDY");
            String jobn = printRes();

            //get list of servers
            int nData = getInfo(jobn, 2);

            //quit if no servers or error
            if(nData == -1) {
                sendMsg("QUIT");
            }
            
            sendMsg("OK");
            String reply = null;
            String serverInfo = " ";

            for(int i = 0; i < nData; i++) {
                reply = input.readLine();
                serverInfo += ("--" + reply);
            }

            sendMsg("OK");

            //store data in server array
            String [] server = getLargestServer(serverInfo);
            System.out.println("SERVER: " + Arrays.toString(server));
            printRes();

            while(true) {
                if(jobn.contains("JCPL")) {
                    sendMsg("REDY");
                    jobn = printRes();
                    if(jobn.contains("NONE")) {
                        sendMsg("QUIT");
                        break;
                    }
                    continue;
                }
                String sched = "SCHD " + getJobID(jobn) + " " + server[0] + " " + Integer.parseInt(server[1]);
                System.out.println("SCHED: " + sched);
                sendMsg(sched);

                printRes();

                sendMsg("REDY");

                jobn = printRes();
                System.out.println("JOB ID: " + jobn);

                if(jobn.contains("NONE")) {
                    sendMsg("QUIT");
                    break;
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


