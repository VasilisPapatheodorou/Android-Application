import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class workerNode {
    // Shared memory
    private static Map<String, Map<String, String>> memory = new HashMap<>(); 
    public static void main(String[] args) throws IOException {
        
        @SuppressWarnings("resource")
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
                
                //Initialize result
                Map<String, Map<String, String>> result = new HashMap<>();

                //Depending on the operation we execute the appropriate code
                switch (operation) {
                    case "Add Accommodation":
                        // Read the map sent by the master
                        @SuppressWarnings("unchecked")
                        Map<String, Map<String, String>> data = (Map<String, Map<String, String>>) inputStream.readObject(); //HashMap

                        synchronized (memory) {
                            // Merge the memory with the new data
                            //System.out.println(memory);
                            for (Map.Entry<String, Map<String, String>> entry : memory.entrySet()) {
                                String key = entry.getKey();
                                Map<String, String> value = entry.getValue();
                                System.out.println("Key "+key+" value "+value);
                
                                // Check if the data contains the same key
                                if (data.containsKey(key)) {
                                    // Merge the values for the same key
                                    Map<String, String> mergedValue = new HashMap<>(value);
                                    System.out.println("old "+ mergedValue+" new "+data.get(key));
                                    mergedValue.putAll(data.get(key));
                                    //System.out.println(mergedValue);
                                    result.put(key, mergedValue);
                                } else {
                                    // If data does not contain the key, just add it from memory
                                    result.put(key, value);
                                }
                            }
                            //System.out.println(result);
                            // Add new keys from data to result
                            for (Map.Entry<String, Map<String, String>> entry : data.entrySet()) {
                                String key = entry.getKey();
                                if (!memory.containsKey(key)) {
                                    result.put(key, entry.getValue());
                                }
                            }
                            //System.out.println(result);
                            memory.putAll(result);
                        }
                        break;

                    case "Search Accommodation":
                        // Read the filter sent by the master
                        String filter = input.readLine();
                        String filter2 = input.readLine();
                        // Process the data based on the filter
                        switch (filter) {
                            case "area":

                                for (Map.Entry<String, Map<String, String>> entry : memory.entrySet()) {
                                    String roomName = entry.getKey(); // Get the room name
                                    Map<String, String> roomDetails = entry.getValue(); // Get the room details
                
                                    // Check 
                                    if (filter2.equals(roomDetails.get("area"))) {
                                        // Add the room to the new map
                                        result.put(roomName, roomDetails);
                                    }
                                }
                                break;
                            case "capacity":
                                for (Map.Entry<String, Map<String, String>> entry : memory.entrySet()) {
                                    String roomName = entry.getKey(); // Get the room name
                                    Map<String, String> roomDetails = entry.getValue(); // Get the room details
                
                                    // Check 
                                    if (filter2.equals(roomDetails.get("noOfPersons"))) {
                                        // Add the room to the new map
                                        result.put(roomName, roomDetails);
                                    }
                                }
                                break;
                            case "price":
                                for (Map.Entry<String, Map<String, String>> entry : memory.entrySet()) {
                                    String roomName = entry.getKey(); // Get the room name
                                    Map<String, String> roomDetails = entry.getValue(); // Get the room details
                
                                    // Check 
                                    if (filter2.equals(roomDetails.get("price"))) {
                                        // Add the room to the new map
                                        result.put(roomName, roomDetails);
                                    }
                                }
                                break;
                            case "stars":
                                for (Map.Entry<String, Map<String, String>> entry : memory.entrySet()) {
                                    String roomName = entry.getKey(); // Get the room name
                                    Map<String, String> roomDetails = entry.getValue(); // Get the room details
                
                                    // Check 
                                    if (filter2.equals(roomDetails.get("stars"))) {
                                        // Add the room to the new map
                                        result.put(roomName, roomDetails);
                                    }
                                }
                                break;
                            case "date":
                                for (Map.Entry<String, Map<String, String>> entry : memory.entrySet()) {
                                    String roomName = entry.getKey(); // Get the room name
                                    Map<String, String> roomDetails = entry.getValue(); // Get the room details
                
                                    // Check 
                                    if (filter2.equals(roomDetails.get("date"))) {
                                        // Add the room to the new map
                                        result.put(roomName, roomDetails);
                                    }
                                }
                                break;
                            default:
                                break;
                        }
                        break;
                    case "Rent Accomodation":
                        // Read the filter sent by the master
                        String roomChoice = input.readLine();
                        String startDate = input.readLine();
                        
                        //Format String Date into a LocalDate Object
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
                        LocalDate start = LocalDate.parse(startDate, formatter);
                        System.out.println("Original String: " + startDate);
                        System.out.println("Parsed LocalDate: " + start);
                        
                        String endDate = input.readLine();
                        
                        //Format String Date into a LocalDate Object
                        LocalDate end = LocalDate.parse(endDate, formatter);
                        System.out.println("Original String: " + startDate);
                        System.out.println("Parsed LocalDate: " + end);
                        System.out.println("ok");
                        
                        // Adding booking items to each room
                        synchronized (memory) {
                            for (Map.Entry<String, Map<String, String>> entry : memory.entrySet()) {
                                System.out.println(entry.getValue());
                                System.out.println(entry.getValue().get("bookings"));
                            }
                        }
                        
                    case "Rate Accomodation":

                    case "Show bookings":

                    default:
                        result=memory;
                }

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
    }
}
