import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class workerNode {

    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(12346);
        System.out.println("Worker node started. Waiting for server...");
        
        //Create dynamic memory for the worker
        Map<String,Map<String,String>> Memory = null; 
        
        while (true) {
            Socket serverConnection = serverSocket.accept(); // Accept connection from server
            System.out.println("Server connected: " + serverConnection);

            // Start a new thread to handle each client connection
            Thread clientHandlerThread = new Thread(new ClientHandler(serverConnection));
            clientHandlerThread.start();
        }
    }

    // ClientHandler class to handle each client connection
    private static class ClientHandler implements Runnable {
        private final Socket serverConnection;

        public ClientHandler(Socket serverConnection) {
            this.serverConnection = serverConnection;
        }

        @Override
        public void run() {
            try (
                    BufferedReader input = new BufferedReader(new InputStreamReader(serverConnection.getInputStream()));
                    PrintWriter output = new PrintWriter(serverConnection.getOutputStream(), true);
                    ObjectInputStream inputStream = new ObjectInputStream(serverConnection.getInputStream());
                    ObjectOutputStream outputStream = new ObjectOutputStream(serverConnection.getOutputStream())
            ) {
                // Read operation from server
                String operation = input.readLine();
                System.out.println("Operation received: " + operation);

                // Read the map sent by the master
                Map<String, Map<String, String>> data = (Map<String, Map<String, String>>) inputStream.readObject(); //HashMap

                // Process the data based on the operation
                Map<String, Map<String, String>> result = processOperation(operation, data);

                // Connect to reducer
                Socket reducerSocket = new Socket("localhost", 12347);
                System.out.println("Connected to Reducer");

                // Creating output stream for communication with reducer
                ObjectOutputStream outputToReducer = new ObjectOutputStream(reducerSocket.getOutputStream());

                // Send result to reducer
                outputToReducer.writeObject(result);

                // Close connections
                reducerSocket.close();
                serverConnection.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        // Method to process the operation
        private Map<String, Map<String, String>> processOperation(String operation, Map<String, Map<String, String>> data) {
            // New map to store rooms in Area3
            Map<String, Map<String, String>> result = new HashMap<>();

            if (operation.equals("Add Accommodation")) {
                
            } else if (operation.equals("Rent Accommodation")) {
                for (Map.Entry<String, Map<String, String>> entry : data.entrySet()) {
                    String roomName = entry.getKey(); // Get the room name
                    Map<String, String> roomDetails = entry.getValue(); // Get the room details

                    // Check if the room is in Area3
                    if ("Area3".equals(roomDetails.get("area"))) {
                        // Add the room to the new map
                        result.put(roomName, roomDetails);
                    }
                }
            }
            return result;
        }
    }
}
