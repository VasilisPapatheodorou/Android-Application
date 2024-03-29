import java.io.*;
import java.net.*;

public class client {
    public static void main(String[] args) {
        try {
            Socket socket = new Socket("localhost", 12345); // Connect to localhost on port 12345

            // Creating input and output streams for communication
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter output = new PrintWriter(socket.getOutputStream(), true);

            // Reading option menu from the server
            String serverResponse1;
            if ((serverResponse1 = input.readLine()) != null) {
                System.out.println(serverResponse1);
            }
            
            // Reading client's choice from the console
            BufferedReader consoleInput1 = new BufferedReader(new InputStreamReader(System.in));
            String userInput1 = consoleInput1.readLine();

            // Sending the choice to the server
            output.println(userInput1);

            // Reading answer
            String serverResponse2;
            if ((serverResponse2 = input.readLine()) != null) {
                System.out.println(serverResponse2);
            }
            
            // Reading client's choice from the console
            BufferedReader consoleInput2 = new BufferedReader(new InputStreamReader(System.in));
            String userInput2 = consoleInput2.readLine();

            // Sending the choice to the server
            output.println(userInput2);

            // Receive and print server's response
            String response;
            while ((response = input.readLine()) != null) {
                System.out.println("Server: " + response);
            }

            // Close connections
            input.close();
            output.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

