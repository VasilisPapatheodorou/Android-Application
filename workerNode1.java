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
import java.util.Map;

public class workerNode1 {
    // Shared memory
    private static Map<String, ArrayList<Map<String,String>>> memory = new HashMap<>();
    
    public static void main(String[] args) throws IOException {
        
        @SuppressWarnings("resource")
        ServerSocket serverSocket = new ServerSocket(12355);
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
        private Map<String, ArrayList<Map<String,String>>> memory;
        private ArrayList<Map<String,String>> value;

        public ClientHandler(Socket serverConnection, Map<String, ArrayList<Map<String,String>>> memory) {
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
                //read id 
                Integer id = input.read();

                String operation = input.readLine();
                System.out.println("Operation received: " + operation);
                
                //Initialize result
                Map<String, ArrayList<Map<String,String>>> result = new HashMap<>();
                //Depending on the operation we execute the appropriate code
                switch (operation) {
                    case "Add Accomodation":
                        //value to define if manager already exists
                        boolean found=false;
                        //read person 
                        String person = input.readLine();
                        // Read the map sent by the master
                        Map<String, String> data = (Map<String, String>) inputStream.readObject(); //HashMap
                        synchronized (memory) {
                            // Iterate over the entries of the data map
                            for (Map.Entry<String, ArrayList<Map<String,String>>> entry : memory.entrySet()) {
                                String key = entry.getKey();
                                if (person==key){
                                    entry.getValue().add(data);
                                    found=true;
                                }
                            }
                            if (!found){
                                value.clear();
                                value.add(data);
                                memory.put(person, value);
                            }

                            }
                        result=memory;
                        break;

                    case "Search Accomodation":
                        // Read the filter sent by the master
                        String filter = input.readLine();
                        String filter2 = input.readLine();
                        // Process the data based on the filter
                        switch (filter) {
                            case "area":
                                for (Map.Entry<String, ArrayList<Map<String,String>>> entry : memory.entrySet()) {
                                    
                                    String roomName = entry.getKey(); // Get the room name
                                    ArrayList<Map<String,String>> roomDetails = entry.getValue(); // Get the room details
                                    // List to store filtered items for the current room
                                    ArrayList<Map<String,String>> filteredItems = new ArrayList<>();
                                    // Check 
                                    for(Map<String,String> item : roomDetails){
                                        if (filter2.equals(item.get("area"))) {
                                            System.out.println(item);
                                            filteredItems.add(item);
                                        }
                                        
                                    }
                                    // Check if any items were filtered for the current room
                                    if (!filteredItems.isEmpty()) {
                                        // Add the filtered items to the result map with the roomName as key
                                        result.put(roomName, filteredItems);
                                    }
                                }
                                break;
                            case "capacity":
                                for (Map.Entry<String, ArrayList<Map<String,String>>> entry : memory.entrySet()) {
                                        
                                    String roomName = entry.getKey(); // Get the room name
                                    ArrayList<Map<String,String>> roomDetails = entry.getValue(); // Get the room details
                                    // List to store filtered items for the current room
                                    ArrayList<Map<String,String>> filteredItems = new ArrayList<>();
                                    // Check 
                                    for(Map<String,String> item : roomDetails){
                                        if (filter2.equals(item.get("noOfPersons"))) {
                                            System.out.println(item);
                                            filteredItems.add(item);
                                        }
                                        
                                    }
                                    // Check if any items were filtered for the current room
                                    if (!filteredItems.isEmpty()) {
                                        // Add the filtered items to the result map with the roomName as key
                                        result.put(roomName, filteredItems);
                                    }
                                }
                                break;
                            case "price":
                                for (Map.Entry<String, ArrayList<Map<String,String>>> entry : memory.entrySet()) {
                                        
                                    String roomName = entry.getKey(); // Get the room name
                                    ArrayList<Map<String,String>> roomDetails = entry.getValue(); // Get the room details
                                    // List to store filtered items for the current room
                                    ArrayList<Map<String,String>> filteredItems = new ArrayList<>();
                                    // Check 
                                    for(Map<String,String> item : roomDetails){
                                        if (filter2.equals(item.get("price"))) {
                                            System.out.println(item);
                                            filteredItems.add(item);
                                        }
                                        
                                    }
                                    // Check if any items were filtered for the current room
                                    if (!filteredItems.isEmpty()) {
                                        // Add the filtered items to the result map with the roomName as key
                                        result.put(roomName, filteredItems);
                                    }
                                }
                                break;
                            case "stars":
                                for (Map.Entry<String, ArrayList<Map<String,String>>> entry : memory.entrySet()) {
                                        
                                    String roomName = entry.getKey(); // Get the room name
                                    ArrayList<Map<String,String>> roomDetails = entry.getValue(); // Get the room details
                                    // List to store filtered items for the current room
                                    ArrayList<Map<String,String>> filteredItems = new ArrayList<>();
                                    // Check 
                                    for(Map<String,String> item : roomDetails){
                                        if (filter2.equals(item.get("stars"))) {
                                            System.out.println(item);
                                            filteredItems.add(item);
                                        }
                                        
                                    }
                                    // Check if any items were filtered for the current room
                                    if (!filteredItems.isEmpty()) {
                                        // Add the filtered items to the result map with the roomName as key
                                        result.put(roomName, filteredItems);
                                    }
                                }
                                break;
                            case "date":
                                for (Map.Entry<String, ArrayList<Map<String,String>>> entry : memory.entrySet()) {
                                        
                                    String roomName = entry.getKey(); // Get the room name
                                    ArrayList<Map<String,String>> roomDetails = entry.getValue(); // Get the room details
                                    // List to store filtered items for the current room
                                    ArrayList<Map<String,String>> filteredItems = new ArrayList<>();
                                    // Check 
                                    for(Map<String,String> item : roomDetails){
                                        if (filter2.equals(item.get("date"))) {
                                            System.out.println(item);
                                            filteredItems.add(item);
                                        }
                                        
                                    }
                                    // Check if any items were filtered for the current room
                                    if (!filteredItems.isEmpty()) {
                                        // Add the filtered items to the result map with the roomName as key
                                        result.put(roomName, filteredItems);
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
                            for (Map.Entry<String, ArrayList<Map<String, String>>> entry : memory.entrySet()) {
                                for(Map<String, String> item : entry.getValue()){}
                                System.out.println(entry.getValue());
                                //System.out.println(entry.getValue().get("bookings"));
                            }
                        }
                        
                    case "Rate Accomodation":

                    case "Show bookings":

                    default:
                        result=memory;
                }

                // Connect to reducer
                Socket reducerSocket = new Socket("localhost", 12350);
                System.out.println("Connected to Reducer");

                // Creating output stream for communication with reducer
                ObjectOutputStream outputToReducer = new ObjectOutputStream(reducerSocket.getOutputStream());

                // Send result to reducer
                Map<Integer,Map<String, ArrayList<Map<String,String>>>> final_result =new HashMap<>();
                final_result.put(id,result);
                outputToReducer.writeObject(final_result);

                // Close connections
                reducerSocket.close();
                serverConnection.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
