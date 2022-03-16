import java.net.*;
import java.io.*;

public class Server {
    Socket s;
    ServerSocket server;
    DataInputStream input;
    DataOutputStream reply;

    public Server(int port) {
        try {
            server = new ServerSocket(port);
            System.out.println("Server is online, waiting for client..");

            //wait for client to conncect
            s = server.accept();
            System.out.println("Client successfully connected");
            
            input = new DataInputStream(new BufferedInputStream(s.getInputStream()));
            reply = new DataOutputStream(s.getOutputStream());

            String msg = "";

            while(!msg.equals("END")) {
                try {
                    msg = input.readUTF();
                    System.out.println("Message: " + msg);

                    reply.writeUTF("Server received");
                    reply.flush();
                }
                catch(Exception e) {
                    System.out.println("error reading message");
                }
            }

            System.out.println("Job finished, terminating connection");

            s.close();
            input.close();
            reply.close();

        }
        catch(Exception e) {
            System.out.println("Connection error!");
        }
    }

    public static void main(String args[]) {
        Server s = new Server(8000);
    }
}