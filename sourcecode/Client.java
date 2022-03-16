import java.net.*;
import java.io.*;

public class Client {
    Socket s;
    BufferedReader input;
    DataInputStream reply;
    DataOutputStream output;

    public Client(String hostid, int port) {

        //try to connect to server
        try {
            s = new Socket(hostid, port);
            System.out.println("Connection Success!");

            input = new BufferedReader(new InputStreamReader(System.in));
            output = new DataOutputStream(s.getOutputStream());
            reply = new DataInputStream(s.getInputStream());
            
        }
        catch(Exception e) {
            System.out.println("Connection error!");
        }

        //input message and response
        String message = "";
        String response = "";

        while(!message.equals("END")) {
            try {
                message = input.readLine();
                output.writeUTF(message);
                output.flush();

                response = reply.readUTF();
                System.out.println("Status: " + response);
            }
            catch(Exception e) {
                System.out.println("Sending failed");
            }
        }
        try {
            System.out.println("terminating");
            input.close();
            output.close();
            s.close();
        }
        catch (Exception e) {
            System.out.println("Failed to close");
        }
    }

    public static void main(String args[]) {
        Client c = new Client("127.0.0.1", 8000);
    }
}