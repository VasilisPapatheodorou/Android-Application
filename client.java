import java.io.*;
import java.net.*;

public class client {
    public static void main(String[] args) {
        try {
            Socket socket = new Socket("localhost", 12345); // Connect to localhost on port 12345

            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter output = new PrintWriter(socket.getOutputStream(), true);

            // Read and print option menu from the server
            readAndPrintResponse(input);

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


