import java.io.*;
import java.net.*;

public class client {
    public static void main(String[] args) {
        try {
            Socket socket = new Socket("localhost", 12345); // Connect to localhost on port 12345

            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter output = new PrintWriter(socket.getOutputStream(), true);

            // Send the option menu to the client
            System.out.println("Welcome to Booking App!");
            System.out.println("Choose action by number");
            System.out.println("1. Add Accommodation");
            System.out.println("2. Rent accomodation");
            System.out.println("3. Rate accomodation");
            System.out.println("4. Search Accommodation");
            System.out.println("5. Show reservations");
            System.out.println("6. Exit");

            // Read user input from the console and send it to the server
            
            String userInput1 = readUserInput();
            sendToServer(output, userInput1);

            // Read and print server response
            readAndPrintResponse(input);

            String userInput2 = readUserInput();
            sendToServer(output, userInput2);

            readAndPrintResponse(input);

            String userInput3 = readUserInput();
            sendToServer(output, userInput3);

            readAndPrintResponse(input);

            String userInput4 = readUserInput();
            sendToServer(output, userInput4);

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

    private static void readAndPrintResponse(BufferedReader input) throws IOException {
        String serverResponse;
        if ((serverResponse = input.readLine()) != null) {
            System.out.println(serverResponse);
        }
    }

    private static String readUserInput() throws IOException {
        BufferedReader consoleInput = new BufferedReader(new InputStreamReader(System.in));
        return consoleInput.readLine();
    }

    private static void sendToServer(PrintWriter output, String userInput) {
        output.println(userInput);
    }
}


