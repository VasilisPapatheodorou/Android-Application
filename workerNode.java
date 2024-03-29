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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class workerNode {
    public static void main(String[] args) throws InterruptedException, ExecutionException, ClassNotFoundException {
        ExecutorService executor = Executors.newCachedThreadPool(); // Creates a thread pool
        Map<String,Map<String,String>> Memory = null; 
        try {
            try (ServerSocket serverSocket = new ServerSocket(12346)) {
                System.out.println("Worker node started. Waiting for server...");

                while (true) {
                    Socket serverConnection = serverSocket.accept(); // Accept connection from server
                    System.out.println("Server connected: " + serverConnection);

                    // Create input and output streams for communication with server
                    BufferedReader input = new BufferedReader(new InputStreamReader(serverConnection.getInputStream()));
                    PrintWriter output = new PrintWriter(serverConnection.getOutputStream(), true);
                    
                    // Creating input and output streams for communication
                    ObjectInputStream inputStream = new ObjectInputStream(serverConnection.getInputStream());
                    ObjectOutputStream outputStream = new ObjectOutputStream(serverConnection.getOutputStream());

                    // Read operation from server
                    String operation = input.readLine();
                    System.out.println("Operation received: " + operation);

                    // Read the map sent by the master
                    Map<String, Map<String,String>> data = (Map<String, Map<String,String>>) inputStream.readObject(); //HashMap
                    
                    //Update the memory

                    // Start a new thread to handle master requests
                    MasterHandler masterHandler = new MasterHandler(operation,data);
                    Future<Map<String, Map<String,String>>> future = executor.submit(masterHandler);

                    // Retrieve the result from the future when it's available
                    Map<String, Map<String,String>> result = future.get();

                    // Connect to reducer
                    Socket ReducerSocket = new Socket("localhost", 12347);
                    System.out.println("Connected to Reducer");

                    // Creating output stream for communication with reducer
                    ObjectOutputStream outputToReducer = new ObjectOutputStream(ReducerSocket.getOutputStream());

                    // Send result to reducer
                    outputToReducer.writeObject(result);

                    // Close connections
                    ReducerSocket.close();
                    input.close();
                    output.close();
                    serverConnection.close();
                }
            }
        }catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }finally {
            // Shutdown the executor service
            executor.shutdown();
        }
    }

    // ClientHandler class to handle each client connection
    private static class MasterHandler implements Callable <Map<String,Map<String,String>>>{
        String operation;
        Map<String,Map<String,String>> data;

        public MasterHandler(String operation,Map<String,Map<String,String>> data) {
            this.operation = operation;
            this.data= data;
        }

        @Override
        public  Map<String,Map<String,String>> call() {
            // New map to store rooms in Area3
            Map<String, Map<String, String>> result = new HashMap<>();
            
            if (operation.equals("Add Accommodation")) {
                for (Map.Entry<String, Map<String, String>> entry : data.entrySet()) {
                    String roomName = entry.getKey(); // Get the room name
                    Map<String, String> roomDetails = entry.getValue(); // Get the room details

                    // Check if the room is in Area3
                    if ("Area3".equals(roomDetails.get("area"))) {
                        // Add the room to the new map
                        result.put(roomName, roomDetails);
                    }
                }
            } else if (operation.equals("Rent Accommodation")) {
                // Perform "Rent Accommodation" operation
                // Simulated result
                
            }
            return result;
        }
    }
}

