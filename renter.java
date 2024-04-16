import java.io.*;
import java.net.*;


//javac -cp json-simple-1.1.1.jar renter.java
//java -cp .;json-simple-1.1.1.jar client

public class renter {
    @SuppressWarnings("unused")
    public static void main(String[] args) throws ClassNotFoundException {
        try {
            Socket socket = new Socket("localhost", 12345); // Connect to localhost on port 12345

            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
            ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());

            // Send the option menu to the client
            System.out.println("Welcome to Booking App!");
            System.out.println("Choose action by number");
            System.out.println("1. Rent accomodation");
            System.out.println("2. Rate accomodation");
            System.out.println("3. Search Accomodation");
            System.out.println("4. Exit");

            // Read user input from the console and send it to the server
            
            String userInput1 = readUserInput();
            System.out.println(userInput1);
            sendToServer(output, userInput1);

            // Read and print server response
            readAndPrintResponse(input);
            
            String userInput2 = readUserInput();

            sendToServer(output, userInput2);
  
            readAndPrintResponse(input);

            String userInput3 = readUserInput();
            sendToServer(output, userInput3);

            readAndPrintResponse(input);

            switch (userInput1){
                case "Rent Accomodation":
                    String userInput4 = readUserInput();
                    sendToServer(output, userInput4);

            }

            // Receive and print server's response
            String response;
            while ((response = input.readLine()) != null) {
                System.out.println("Server: " + response);
            }

            System.out.println(input.readLine());

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