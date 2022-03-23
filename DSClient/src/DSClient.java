import java.net.*;
import java.io.*;

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
        String response = new String();
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

    //getData(): get data from jobn response if 1 or all if 0, 
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
    public static String getLargestServer(String data) {
        String [] list = data.split("--");
        String largest = list[list.length-2];
        
        return largest;
    }

    //DSClient(): Class instantiates server connection and communicates
    //with DS Server 
    public DSClient(String hostid, int port) {
	    
        //try to connect to server
        Boolean result = serverConn(hostid, port);

        if(result == false) {
            return;
        }

        //input message
        String message = "";

        try {
            if(!initialise()) {
                return;
            }

            sendMsg("REDY");
            message = printRes();

            int nData = getInfo(message, 2);

            if(nData == -1) {
                sendMsg("QUIT");
            }
            
            String serverInfo = "";

            while(!serverInfo.contains(".")) {
                sendMsg("OK");
                serverInfo=serverInfo + "--" + printRes();
            }
            
            System.out.println(getLargestServer(serverInfo));

            if(printRes().equals(".")){ 
                sendMsg("QUIT");
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


