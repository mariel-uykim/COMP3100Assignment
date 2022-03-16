
import java.net.*;
import java.io.*;

public class DSClient {
    Socket s;
    BufferedReader input;
    BufferedReader reply;
    DataOutputStream output;

    public DSClient(String hostid, int port) {

        //try to connect to server
        try {
            s = new Socket(hostid, port);
            System.out.println("Connection Success!");

            input = new BufferedReader(new InputStreamReader(s.getInputStream()));
            output = new DataOutputStream(s.getOutputStream());
            reply = new BufferedReader(new InputStreamReader(s.getInputStream()));
            
        }
        catch(Exception e) {
            System.out.println(e);
        }

        //input message and response
        String message = "";
        String response = "";

        while(!response.equals("QUIT")) {
            try {
                output.write(("HELO").getBytes());
                output.flush();

                response = reply.readLine();
                System.out.println("Status: " + response);
            }
            catch(Exception e) {
                System.out.println(e);
            }
        }
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

    public static void main(String args[]) {
        DSClient c = new DSClient("127.0.0.1",50000);
    }
}
