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
import java.util.stream.Stream;

public class workerNode {
    // Shared memory
    private static Map<String, Map<String, String>> memory = new HashMap<>(); 
    public static void main(String[] args) throws IOException {
        
        ServerSocket serverSocket = new ServerSocket(12346);
        System.out.println("Worker node started. Waiting for server...");
        
        while (true) {
            Socket serverConnection = serverSocket.accept(); // Accept connection from server
            System.out.println("Server connected: " + serverConnection);

            // Start a new thread to handle each client connection
            Thread clientHandlerThread = new Thread(new ClientHandler(serverConnection,memory));
            clientHandlerThread.start();
        }
    }

    // ClientHandler class to handle each client connection
    private static class ClientHandler implements Runnable {
        private final Socket serverConnection;
        private Map<String, Map<String, String>> memory;

        public ClientHandler(Socket serverConnection, Map<String, Map<String, String>> memory) {
            this.serverConnection = serverConnection;
            this.memory = memory;
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
                Map<String, Map<String, String>> result = processOperation(operation, data, memory);

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
        private Map<String, Map<String, String>> processOperation(String operation, Map<String, Map<String, String>> data, Map<String, Map<String, String>> memory) {
            // New map to store rooms in Area3
            Map<String, Map<String, String>> result = new HashMap<>();

            if (operation.equals("Add Accommodation")) {
                synchronized (memory) {
                    // Merge the memory with the new data
                    for (Map.Entry<String, Map<String, String>> entry : memory.entrySet()) {
                        String key = entry.getKey();
                        Map<String, String> value = entry.getValue();
        
                        // Check if the data contains the same key
                        if (data.containsKey(key)) {
                            // Merge the values for the same key
                            Map<String, String> mergedValue = new HashMap<>(value);
                            mergedValue.putAll(data.get(key));
                            result.put(key, mergedValue);
                        } else {
                            // If data does not contain the key, just add it from memory
                            result.put(key, value);
                        }
                    }
        
                    // Add new keys from data to result
                    for (Map.Entry<String, Map<String, String>> entry : data.entrySet()) {
                        String key = entry.getKey();
                        if (!memory.containsKey(key)) {
                            result.put(key, entry.getValue());
                        }
                    }
        
                    // Update memory with the merged result
                    memory = result;
                }
                return memory;
            } 
            else if (operation.equals("Search Accommodation")) {
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
            else if (operation.equals("Rent Accommodation")) {
                
            }
            else if (operation.equals("Rate Accommodation")) {
                
            }
            else if (operation.equals("Add Accommodation")) {
                
            }
            return result;
        }
    }
}
