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

public class workerNode3 {
    // Shared memory
    private static Map<String, ArrayList<Map<String,Object>>> memory = new HashMap<>();
    
    public static void main(String[] args) throws IOException {
        
        @SuppressWarnings("resource")
        ServerSocket serverSocket = new ServerSocket(12348);
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
        private Map<String, ArrayList<Map<String,Object>>> memory;
        private ArrayList<Map<String,Object>> value;

        public ClientHandler(Socket serverConnection, Map<String, ArrayList<Map<String,Object>>> memory) {
            this.serverConnection = serverConnection;
            this.memory = memory;
            this.value = new ArrayList<Map<String,Object>>();
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
                //Integer id = input.read();
                Integer id = (Integer) inputStream.readObject();

                String operation = (String) inputStream.readObject();
                System.out.println("Operation received: " + operation);
                
                //Initialize result
                Map<String, ArrayList<Map<String,Object>>> result = new HashMap<>();
                //Depending on the operation we execute the appropriate code
                switch (operation) {
                    case "Add Accomodation":
                    //value to define if manager already exists
                    boolean found=false;
                    //read person 
                    String person = (String) inputStream.readObject();
                    // Read the map sent by the master
                    Map<String, String> inputData = (Map<String, String>) inputStream.readObject(); //HashMap
                    // New map of type Map<String, Object>
                    Map<String, Object> data = new HashMap<>();
                    
                    // Iterate through each entry in the original map
                    for (Map.Entry<String, String> entry : inputData.entrySet()) {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        
                        // Convert the string value to an object and put it into the new map
                        if(key.equals("bookings")){
                            // Convert the string value to an ArrayList and put it into the new map
                            Map<LocalDate, LocalDate> bookingsMap = new HashMap<>();
                            data.put(key, bookingsMap);
                        }else{
                            data.put(key, (Object) value);
                        }
                        
                    }    
                    synchronized (memory) {
                        // Iterate over the entries of the data map
                        for (Map.Entry<String, ArrayList<Map<String,Object>>> entry : memory.entrySet()) {
                            String key = entry.getKey();
                            if (person.equals(key)){
                                entry.getValue().add(data);
                                found=true;
                            }
                        }
                        if (!found){
                            if(value!=null){
                                value.clear();
                            }
                            value.add(data);
                            memory.put(person, value);
                        }
                    }
                    //System.out.println(memory);
                    break;

                    case "Search Accomodation":
                        // Read the filter sent by the master
                        System.out.println("ok");
                        String filter = (String) inputStream.readObject();
                        String filter2 = (String) inputStream.readObject();
                        // Process the data based on the filter
                        switch (filter) {
                            case "area":
                                for (Map.Entry<String, ArrayList<Map<String,Object>>> entry : memory.entrySet()) {
                                    
                                    String roomName = entry.getKey(); // Get the room name
                                    ArrayList<Map<String,Object>> roomDetails = entry.getValue(); // Get the room details
                                    // List to store filtered items for the current room
                                    ArrayList<Map<String,Object>> filteredItems = new ArrayList<>();
                                    // Check 
                                    for(Map<String,Object> item : roomDetails){
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
                                for (Map.Entry<String, ArrayList<Map<String,Object>>> entry : memory.entrySet()) {
                                        
                                    String roomName = entry.getKey(); // Get the room name
                                    ArrayList<Map<String,Object>> roomDetails = entry.getValue(); // Get the room details
                                    // List to store filtered items for the current room
                                    ArrayList<Map<String,Object>> filteredItems = new ArrayList<>();
                                    // Check 
                                    for(Map<String,Object> item : roomDetails){
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
                                for (Map.Entry<String, ArrayList<Map<String,Object>>> entry : memory.entrySet()) {
                                        
                                    String roomName = entry.getKey(); // Get the room name
                                    ArrayList<Map<String,Object>> roomDetails = entry.getValue(); // Get the room details
                                    // List to store filtered items for the current room
                                    ArrayList<Map<String,Object>> filteredItems = new ArrayList<>();
                                    // Check 
                                    for(Map<String,Object> item : roomDetails){
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
                                for (Map.Entry<String, ArrayList<Map<String,Object>>> entry : memory.entrySet()) {
                                        
                                    String roomName = entry.getKey(); // Get the room name
                                    ArrayList<Map<String,Object>> roomDetails = entry.getValue(); // Get the room details
                                    // List to store filtered items for the current room
                                    ArrayList<Map<String,Object>> filteredItems = new ArrayList<>();
                                    // Check 
                                    for(Map<String,Object> item : roomDetails){
                                        if (filter2.equals(item.get("stars").toString())) {
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
                                for (Map.Entry<String, ArrayList<Map<String,Object>>> entry : memory.entrySet()) {
                                        
                                    String roomName = entry.getKey(); // Get the room name
                                    ArrayList<Map<String,Object>> roomDetails = entry.getValue(); // Get the room details
                                    // List to store filtered items for the current room
                                    ArrayList<Map<String,Object>> filteredItems = new ArrayList<>();
                                    // Check 
                                    for(Map<String,Object> item : roomDetails){
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
                        connectReducer(id,result);
                        break;
                    case "Rent Accomodation":
                        // Read the filter sent by the master
                        String roomChoice = (String) inputStream.readObject();
                        String startDate = (String) inputStream.readObject();
                        
                        //Format String Date into a LocalDate Object
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
                        LocalDate start = LocalDate.parse(startDate, formatter);
                        System.out.println("Original String: " + startDate);
                        System.out.println("Parsed LocalDate: " + start);
                        
                        String endDate = (String) inputStream.readObject();
                        
                        //Format String Date into a LocalDate Object
                        LocalDate end = LocalDate.parse(endDate, formatter);
                        System.out.println("Original String: " + startDate);
                        System.out.println("Parsed LocalDate: " + end);
                        System.out.println("ok");
                        
                        // Adding booking items to each room
                        synchronized (memory) {
                            for (Map.Entry<String, ArrayList<Map<String, Object>>> entry : memory.entrySet()) {
                                for(Map<String, Object> item : entry.getValue()){
                                    if(item.get("room").equals(roomChoice)){
                                        System.out.println(item.get("bookings").getClass());
                                        System.out.println(item.get("bookings"));
                                    }
                                    
                                }                                
                            }
                        }
                        connectReducer(id,result);
                        break;
                    case "Add dates":
                        // Read the filter sent by the master
                        String managerRoomChoice = (String) inputStream.readObject();
                        String managerStartDate = (String) inputStream.readObject();
                        
                        //Format String Date into a LocalDate Object
                        DateTimeFormatter managerFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
                        LocalDate managerStart = LocalDate.parse(managerStartDate, managerFormatter);
                        System.out.println("Original String: " + managerStartDate);
                        System.out.println("Parsed LocalDate: " + managerStart);
                        
                        String managerEndDate = (String) inputStream.readObject();
                        
                        //Format String Date into a LocalDate Object
                        LocalDate managerEnd = LocalDate.parse(managerEndDate, managerFormatter);
                        System.out.println("Original String: " + managerEndDate);
                        System.out.println("Parsed LocalDate: " + managerEnd);
                        System.out.println("ok");

                        // Adding booking items to each room
                        synchronized (memory) {
                            for (Map.Entry<String, ArrayList<Map<String, Object>>> entry : memory.entrySet()) {
                                for(Map<String, Object> item : entry.getValue()){
                                    if(item.get("room").equals(managerRoomChoice)){
                                        // Initialize bookings map if not already initialized
                                        Map<LocalDate, LocalDate> bookings = (Map<LocalDate, LocalDate>) item.getOrDefault("bookings", new HashMap<>());
                                        
                                        // Add managerStartDate and managerEndDate to the bookings map
                                        bookings.put(managerStart, managerEnd);
                                        
                                        // Update item with the modified bookings map
                                        item.put("bookings", bookings);
                                    }   
                                }                                
                            }
                        }
                        System.out.println(memory);
                        break;
                    case "Rate Accomodation":
                        // Read the room sent by the master
                        String room = (String) inputStream.readObject();
                        // Read the rating sent by the master
                        Integer rating = (Integer) inputStream.readObject();
                        synchronized (memory) {
                            for (Map.Entry<String, ArrayList<Map<String, Object>>> entry : memory.entrySet()) {
                                for(Map<String, Object> rooms : entry.getValue()){
                                    if(rooms.get("room").equals(room)){
                                        //Convert the value of starts to Integer
                                        Integer stars = Integer.parseInt(rooms.get("stars").toString());
                                        //Convert the value of noOfReviews to Integer
                                        Integer reviews = Integer.parseInt(rooms.get("noOfReviews").toString())+1;
                                        //Calculate new Rating
                                        Integer newRating = (rating+stars)/reviews;
                                        //Update Rating and noOfReviews
                                        rooms.replace("stars", newRating);
                                        rooms.replace("noOfReviews", reviews);
                                    }
                                }
                            }                                
                        }
                        break;
                    case "Show reservations":
                        // Read the name sent by the master
                        String name = (String) inputStream.readObject();
                        // Searching bookings for name
                        synchronized (memory) {
                            for (Map.Entry<String, ArrayList<Map<String, Object>>> entry : memory.entrySet()) {
                                if(entry.getKey().equals(name)){
                                    result.put(name, entry.getValue());
                                }
                            }                                
                        }
                        connectReducer(id,result);
                    default:
                        result=memory;
                }
                serverConnection.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
    private static void connectReducer(Integer id, Map<String, ArrayList<Map<String,Object>>> result) throws IOException {
        // Connect to reducer
        Socket reducerSocket = new Socket("localhost", 12350);
        System.out.println("Connected to Reducer");

        // Creating output stream for communication with reducer
        ObjectOutputStream outputToReducer = new ObjectOutputStream(reducerSocket.getOutputStream());

        // Send result to reducer
        Map<Integer,Map<String, ArrayList<Map<String,Object>>>> final_result =new HashMap<>();
        final_result.put(id,result);
        outputToReducer.writeObject(final_result);

        // Close connections
        reducerSocket.close();
    }
}
