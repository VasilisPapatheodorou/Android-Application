import java.io.*;
import java.net.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class master {

    private static Integer id=0;

    // Define main method
    public static void main(String[] args) throws IOException {

        // Initialize server socket and accept incoming connections 
        try {
            int serverPort = Integer.parseInt(ConfigManager.getInstance().getProperty("serverPort"));
            try (ServerSocket serverSocket = new ServerSocket(serverPort)) {
                System.out.println("Server started. Waiting for a client...");

                while (true) {
                    
                    // Accept client connection
                    Socket clientSocket = serverSocket.accept(); 
                    System.out.println("Client connected: " + clientSocket);
                    id++;
                    System.out.println(id);
                    
                    // Start a new thread to handle client requests
                    ClientHandler clientHandler = new ClientHandler(clientSocket,id);
                    new Thread(clientHandler).start();
                    
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // ClientHandler class to handle each client connection
    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;
        Integer id;
        Object result;

        public ClientHandler(Socket clientSocket, Integer id) {
            this.clientSocket = clientSocket;
            this.id=id;
        }

        @Override
        public void run() {
            try {
                // Creating input and output streams for communication
                ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());
                ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
                
                // Read client's choice
                Object clientChoice = inputStream.readObject();
                System.out.println(clientChoice);
                processClientChoice(clientChoice, inputStream, outputStream);

                // Close connections
                clientSocket.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        /**
         * Processes the client's choice and performs the corresponding operation.
         *
         * @param clientChoice The client's choice of operation.
         * @param inputStream The input stream to read data from the client.
         * @param outputStream The output stream to send data to the client.
        */

        @SuppressWarnings("unchecked")
        private void processClientChoice(Object clientChoice, ObjectInputStream inputStream, ObjectOutputStream outputStream) throws ClassNotFoundException, IOException{
            //Number of Nodes
            Integer totalNodes = Integer.parseInt(ConfigManager.getInstance().getProperty("workerNodeCount"));
            // Process client's choice
            switch (clientChoice.toString()) {
                case "Add Accomodation":
                    // Perform "Add Accommodation" operation
                    addAccommodation(inputStream, outputStream);
                    break;
                case "Rent Accomodation":
                    // Read client's choice
                    Object choice = inputStream.readObject();
                    Object beginning = inputStream.readObject();
                    Object ending = inputStream.readObject();
                    int rentNode = Hash(choice.toString(), totalNodes);
                    // Connect to worker node and perform "Rent Accommodation" operation
                    rentAccomodation("Rent Accomodation",choice.toString(),beginning.toString(),ending.toString(),id,rentNode);
                    break;
                case "Rate accomodation":
                    // Read client's choice
                    Object roomChoice = inputStream.readObject();
                    // Read rating
                    Object rating = inputStream.readObject();
                    //Find in which worker the room belongs
                    int node = Hash(roomChoice.toString(), totalNodes);
                    // Connect to worker node and perform "Rate Accommodation" operation
                    rateAccomodation("Rate Accomodation",roomChoice.toString(),Integer.parseInt(rating.toString()),id,node);
                    break;
                case "Search Accomodation":
                    // Read client filter
                    Map<String,Object> filters = (Map<String,Object>) inputStream.readObject();
                    // Connect to worker node and perform "Search Accommodation" operation
                    searchAccomodation("Search Accomodation",filters,id);
                    //building connection with reducer
                    result = connectWithReducer();
                    if(result instanceof Map){
                        System.out.println("map");
                        Map<Integer,Object> mp = (Map<Integer,Object>) result;
                        System.out.println(mp);
                        for (Map.Entry<Integer, Object> entry : mp.entrySet()) {
                            if(entry.getValue() instanceof ArrayList){
                                ArrayList<Room> np = (ArrayList<Room>) entry.getValue();
                                System.out.println(np);
                                Map<String,Map<String, String>> results = deserializable(np);
                                outputStream.writeObject(results);
                            }
                        }
                    }
                    
                    break;
                case "Show reservations":
                    // Ask for name
                    Object name = inputStream.readObject();
                    // Connect to worker node and perform "Show reservations" operation
                    showReservations("Show reservations",name.toString(),id);
                    //building connection with reducer
                    result = connectWithReducer();
                    if(result instanceof Map){
                        Map<Integer,Object> mp = (Map<Integer,Object>) result;
                        for (Map.Entry<Integer, Object> entry : mp.entrySet()) {
                            if(entry.getValue() instanceof Map){
                                Map<String,Map<LocalDate,LocalDate>> np = (Map<String,Map<LocalDate,LocalDate>>) entry.getValue();
                                outputStream.writeObject(np);
                            }
                        }
                    }
                    outputStream.writeObject(result);
                    break;
                case "Add dates":
                    // Ask for room to add dates
                    Object choice2 = inputStream.readObject();
                    Object beginning1 = inputStream.readObject();
                    Object ending1 = inputStream.readObject();
                    // Connect to worker node and perform "Add dates" operation
                    addDates("Add dates",choice2.toString(),beginning1.toString(),ending1.toString(),id);
                    break;
                case "Total Output":
                    //Ask for the date
                    Object beginning2 = inputStream.readObject();
                    Object ending2 = inputStream.readObject();
                    // Connect to worker node and perform "Total Output" operation
                    totalOutput("Total Output",beginning2.toString(),ending2.toString(),id);
                    result = connectWithReducer();
                    if(result instanceof Map){
                        Map<Integer,Object> mp = (Map<Integer,Object>) result;
                        for (Map.Entry<Integer, Object> entry : mp.entrySet()) {
                            if(entry.getValue() instanceof Map){
                                Map<String,Integer> np = (Map<String,Integer>) entry.getValue();
                                outputStream.writeObject(np);
                            }
                        }
                    }
                    outputStream.writeObject(result);
                    break;
                default:
                    outputStream.writeUTF("Invalid choice!");
            }
        }

        /**
         * Converts a list of Room objects into a nested map structure.
         *
         * @param np A list of Room objects to be deserialized.
         * @return A nested map where the key is the room number and the value is a map of room attributes.
        */

        private Map<String,Map<String, String>> deserializable(ArrayList<Room> np) {
            Map<String,Map<String, String>> res = new HashMap<>();
            for(Room room:np){
                // Create a map for each room's attributes
                Map<String,String> map = new HashMap<>();
                map.put("area", room.getArea());
                map.put("price", room.getPrice().toString());
                map.put("roomImage", room.getRoomImage());
                map.put("room", room.getRoomNumber());
                map.put("noOfPersons", room.getNoOfPersons().toString());
                map.put("stars", room.getStars().toString());
                res.put(room.getRoomNumber(), map);
            }
            return res;
        }

        /**
         * Reads accommodation data from the input stream and distributes it to worker nodes for insertion.
         *
         * @param inputStream The input stream to read data from.
         * @param outputStream The output stream to write responses to.
        */

        @SuppressWarnings("unchecked")
        private void addAccommodation(ObjectInputStream inputStream, ObjectOutputStream outputStream) throws IOException, ClassNotFoundException {
            //Number of Nodes
            Integer totalNodes = Integer.parseInt(ConfigManager.getInstance().getProperty("workerNodeCount"));
            // Read client's data 
            
            Map<String, ArrayList<Map<String,String>>> Data = (Map<String, ArrayList<Map<String,String>>>) inputStream.readObject();
            ArrayList<Room> rooms = dataToClass(Data);
            for (Room room : rooms) {
                // Determine the worker node to connect to based on room name
                String roomName = room.getRoomNumber();
                int node = Hash(roomName, totalNodes);
                // Print the node number for debugging
                System.out.println(node);
                // Connect to worker node and perform "Add Accommodation" operation
                insertAccomodation("Add Accomodation",room,node);
            }
        }

        /**
         * Connects to a specific worker node and sends a request to rate accommodation.
         *
         * @param operation The operation to be performed (rate accommodation).
         * @param room The room to be rated.
         * @param rating The rating value.
         * @param id The identifier for the operation.
         * @param node The node number to connect to.
        */

        @SuppressWarnings("unused")
        private void rateAccomodation(String operation, String room, Integer rating, Integer id, Integer node) {
            try {

                Socket socket = connectToWorkerNode(node);

                // Creating input and output streams for communication with worker node
                BufferedReader workerInput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter workerOutput = new PrintWriter(socket.getOutputStream(), true);
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

                // Send operation and data to worker node
                sendData(outputStream, id,operation,room,rating);
                
                // Close connections
                closeConnections(socket, workerInput, workerOutput);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Connects to all worker nodes and sends a request to show reservations for a specified user.
         *
         * @param operation The operation to be performed (show reservations).
         * @param name The name of the user whose reservations are to be shown.
         * @param id The identifier for the operation.
        */

        @SuppressWarnings("unused")
        private void showReservations(String operation, String name, Integer id) throws IOException {
            //Number of Nodes
            Integer totalNodes = Integer.parseInt(ConfigManager.getInstance().getProperty("workerNodeCount"));
            try {
                // Connect to worker node
                for(int i=1; i<=totalNodes; i++){
                    Socket socket = connectToWorkerNode(i);

                    // Creating input and output streams for communication with worker node
                    BufferedReader workerInput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter workerOutput = new PrintWriter(socket.getOutputStream(), true);
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

                    // Send operation and data to worker node
                    sendData(outputStream, id,operation,name);
        
                    // Close connections
                    closeConnections(socket, workerInput, workerOutput);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Connects to the reducer and retrieves the result of a reduction operation.
         *
         * @return The result of the reduction operation from the reducer.
        */

        private Object connectWithReducer() throws IOException, ClassNotFoundException{
            // Retrieve the master reducer port from the configuration manager
            int masterReducerPort = Integer.parseInt(ConfigManager.getInstance().getProperty("masterReducerPort"));
            // Create a server socket to accept a connection from the reducer
            try (ServerSocket serverSocket = new ServerSocket(masterReducerPort)) {
                System.out.println("Server started. Waiting for reducer...");
                
                // Accept the reducer connection
                Socket reducerSocket = serverSocket.accept(); 
                System.out.println("Reducer connected: " + reducerSocket);
                
                // Creating input stream for communication with reducer
                ObjectInputStream inputStream = new ObjectInputStream(reducerSocket.getInputStream());
                
                // Read and return the result from the reducer
                Object result =  inputStream.readObject();
                
                // Close the reducer socket
                reducerSocket.close();
                return result;
            }
        }

        /**
         * Connects to a specific worker node and sends a request to insert accommodation data.
         *
         * @param operation The operation to be performed (add accommodation).
         * @param room The Room object containing accommodation details to be inserted.
         * @param node The node number to connect to.
        */
        @SuppressWarnings("unused")
        private void insertAccomodation(String operation,Room room, Integer node) throws ClassNotFoundException {

            try {
                Socket socket = connectToWorkerNode(node);

                // Creating input and output streams for communication with worker node
                BufferedReader workerInput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter workerOutput = new PrintWriter(socket.getOutputStream(), true);
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

                // Send operation and data to worker node
                System.out.println(room);
                sendData(outputStream, id,operation,room);
                
                // Close connections
                closeConnections(socket, workerInput, workerOutput);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Connects to worker nodes and performs a search accommodation operation with specified filters.
         *
         * @param operation The operation to be performed
         * @param filters A map containing filters for the search operation.
         * @param id The identifier for the operation.
         */

        @SuppressWarnings("unused")
        private void searchAccomodation(String operation, Map<String,Object> filters,Integer id) throws ClassNotFoundException, IOException{

            //Number of Nodes
            Integer totalNodes = Integer.parseInt(ConfigManager.getInstance().getProperty("workerNodeCount"));

            try {
                // Iterate through each worker node and establish a connection
                for(int i=1; i<=totalNodes; i++){
                    Socket socket = connectToWorkerNode(i);

                    // Create input and output streams for communication with the worker node
                    BufferedReader workerInput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter workerOutput = new PrintWriter(socket.getOutputStream(), true);
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

                    // Send operation and data to worker node
                    sendData(outputStream, id,operation,filters);
        
                    // Close connections
                    closeConnections(socket, workerInput, workerOutput);
                }
            } catch (IOException e) {
                // Print stack trace if an IOException occurs
                e.printStackTrace();
            }
        }

        /**
         * Connects to a specific worker node and sends a request to rent accommodation for a specified room.
         *
         * @param operation The operation to be performed.
         * @param room The room to be rented.
         * @param beginning The start date for the rental.
         * @param ending The end date for the rental.
         * @param id The identifier for the operation.
         * @param node The node number to connect to.
        */
        
        @SuppressWarnings("unused")
        private void rentAccomodation(String operation, String room, String beginning,String ending,Integer id,Integer node) throws ClassNotFoundException, IOException{
            //Number of Nodes
            Integer totalNodes = Integer.parseInt(ConfigManager.getInstance().getProperty("workerNodeCount"));

            try {
                Socket socket = connectToWorkerNode(node);

                // Create input and output streams for communication with the worker node
                BufferedReader workerInput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter workerOutput = new PrintWriter(socket.getOutputStream(), true);
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

                // Send operation and data to worker node
                sendData(outputStream, id,operation,room,beginning,ending);
                 
                // Close connections
                closeConnections(socket, workerInput, workerOutput);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        private static int Hash(String roomName, int numberOfNodes) {
            // Calculate the hash code for the roomName
            int hashCode = roomName.hashCode();
            // Calculate the modulo operation with numberOfNodes
            return Math.abs(hashCode) % (numberOfNodes)+1;
        }

        /**
         * Connects to a worker node and sends a request to add dates for a specified room.
         *
         * @param operation The operation to be performed.
         * @param room The room for which the dates are being added.
         * @param beginning The start date.
         * @param ending The end date.
         * @param id The identifier for the operation.
        */
        
        @SuppressWarnings("unused")
        private void addDates(String operation, String room, String beginning,String ending,Integer id) throws ClassNotFoundException, IOException{
            //Number of Nodes
            Integer totalNodes = Integer.parseInt(ConfigManager.getInstance().getProperty("workerNodeCount"));
            try {
                // Determine which worker node to connect to using a hash function
                Integer i = Hash(room, totalNodes);
                
                Socket socket = connectToWorkerNode(i);

                // Create input and output streams for communication with the worker node
                BufferedReader workerInput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter workerOutput = new PrintWriter(socket.getOutputStream(), true);
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

                // Send operation and data to worker node
                sendData(outputStream, id,operation,room,beginning,ending);

                // Close connections
                closeConnections(socket, workerInput, workerOutput);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Connects to worker nodes and sends a total output operation request with specified data.
         *
         * @param operation The operation to be performed.
         * @param beginning The start date for the operation.
         * @param ending The end date for the operation.
         * @param id The identifier for the operation.
        */

        @SuppressWarnings("unused")
        private void totalOutput(String operation, String beginning, String ending,Integer id) throws ClassNotFoundException{

            //Number of Nodes
            Integer totalNodes = Integer.parseInt(ConfigManager.getInstance().getProperty("workerNodeCount"));

            try {
                // Iterate through each worker node and establish a connection
                for(int i=1; i<=totalNodes; i++){

                    Socket socket = connectToWorkerNode(i);

                    // Create input and output streams for communication with the worker node
                    BufferedReader workerInput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter workerOutput = new PrintWriter(socket.getOutputStream(), true);
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());


                    // Send the operation and associated data to the worker node
                    sendData(outputStream, id,operation,beginning,ending);

                    // Close input and output streams
                    closeConnections(socket, workerInput, workerOutput);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Converts a nested Map structure into a list of Room objects.
         * 
         * @param Data A map where the key is a person and the value is a list of maps representing room data.
         * @return An ArrayList of Room objects created from the input data.
        */

        private ArrayList<Room> dataToClass(Map<String, ArrayList<Map<String,String>>> Data){

            // Initialize an empty list to hold the Room objects
            ArrayList<Room> rooms = new ArrayList<Room>();

            // Iterate over each entry in the input map
            for (Map.Entry<String, ArrayList<Map<String, String>>> entry : Data.entrySet()) {
                
                String person = entry.getKey();
                ArrayList<Map<String, String>> apartments = entry.getValue();
                
                // Iterate over each apartment data map in the list
                for (Map<String, String> roomData : apartments) {
                    
                    // Extract room attributes from the map
                    String roomName = roomData.get("room");
                    Integer capacity = Integer.parseInt(roomData.get("noOfPersons"));
                    Integer price = Integer.parseInt(roomData.get("price"));
                    String area = roomData.get("area");
                    Integer noOfReviews = Integer.parseInt(roomData.get("noOfReviews"));
                    String image = roomData.get("roomImage");

                    // Create a new Room object with the extracted data
                    Room room = new Room(person, area, price, image, noOfReviews, 0, capacity, roomName);

                    // Add the created Room object to the list
                    rooms.add(room);
                }
            }
            // Return the list of Room objects
            return rooms;
        }
        /**
         * Utility method to connect to a worker node.
         *
         * @param node The node number to connect to.
         * @return The connected Socket.
        */

        private Socket connectToWorkerNode(int node) throws IOException {

            // Retrieve worker node IP and port from the configuration manager for the specified node
            String workerNodeIp = ConfigManager.getInstance().getProperty("workerNodeIP" + node);
            int workerNodePort = Integer.parseInt(ConfigManager.getInstance().getProperty("workerNodePort" + node));

            // Connect to workerNode to insert data
            Socket workerNodeSocket = new Socket(workerNodeIp, workerNodePort);
            System.out.println("Connected to worker node with IP: " + workerNodeIp + " and port " + workerNodePort);

            return workerNodeSocket;
        }

        /**
         * Utility method to send data to a worker node.
         *
         * @param outputStream The ObjectOutputStream to write data to.
         * @param data The data to be sent.
        */

        private void sendData(ObjectOutputStream outputStream, Object... data) throws IOException {
            for (Object obj : data) {
                outputStream.writeObject(obj);
            }
        }

        /**
         * Utility method to close connections to a worker node.
         *
         * @param socket The Socket to be closed.
         * @param inputStream The BufferedReader to be closed.
         * @param outputStream The PrintWriter to be closed.
        */

        private void closeConnections(Socket socket, BufferedReader inputStream, PrintWriter outputStream) throws IOException {
            inputStream.close();
            outputStream.close();
            socket.close();
        }
    }
}
