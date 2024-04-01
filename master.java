import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

//javac -cp json-simple-1.1.1.jar master.java
//java -cp .;json-simple-1.1.1.jar master

public class master {
    public static void main(String[] args) {

        // Create a Random object
        Random random = new Random();
        // Generate a random integer between 2 and 4 to initialize the workers
        int NumberofWorkers = random.nextInt(3) + 2;
        //Initialize workers

        try {

            try (ServerSocket serverSocket = new ServerSocket(12345)) {
                System.out.println("Server started. Waiting for a client...");

                while (true) {
                    Socket clientSocket = serverSocket.accept(); // Accept client connection
                    System.out.println("Client connected: " + clientSocket);

                    // Start a new thread to handle client requests
                    ClientHandler clientHandler = new ClientHandler(clientSocket,NumberofWorkers);
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
        @SuppressWarnings("unused")
        private Integer numOfWorkers;

        public ClientHandler(Socket clientSocket, Integer numOfWorkers) {
            this.clientSocket = clientSocket;
            this.numOfWorkers = numOfWorkers;
        }

        @Override
        public void run() {
            try {
                // Creating input and output streams for communication
                BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);

                // Read client's choice
                String clientChoice = input.readLine();

                // Process client's choice
                switch (clientChoice) {
                    case "1":
                        // Ask for data to add accomodation
                        output.println("Insert Data");
                        // Read client's data (assuming it's the file path)
                        String jsonDataFilePath = input.readLine();
                        // Parse JSON data from the file
                        
                        Map<String, ArrayList<Map<String,String>>> Data = parseJsonFromFile(jsonDataFilePath);
                        // Connect to worker node and perform "Add Accommodation" operation
                        insertAccomodation("Add Accommodation",Data);
                        //build connection with reducer
                        connectWithReducer();
                        break;
                    case "2":

                        // Ask for room to rent
                        output.println("Choose room");
                        // Read client's choice
                        String choice = input.readLine();
                        // Ask for date
                        output.println("Insert beginning of rent");
                        String beginning = input.readLine();
                        output.println("Insert ending of rent");
                        String ending = input.readLine();
                        // Connect to worker node and perform "Search Accommodation" operation
                        rentAccomodation("Rent Accomodation",choice,beginning,ending);
                        //building connection with reducer
                        connectWithReducer();
                        break;
                    case "3":
                        output.println("Rate accomodation");
                        break;
                    case "4":
                        // Ask for data
                        output.println("Choose filter:");
                        // Read client filter
                        String filter = input.readLine();
                        // Ask for data
                        output.println("Insert "+filter+" of choice:");
                        String filter2 = input.readLine();
                        // Connect to worker node and perform "Search Accommodation" operation
                        searchAccomodation("Search Accommodation",filter,filter2);
                        //building connection with reducer
                        connectWithReducer();
                        break;
                    case "5":
                        output.println("Show reservations");
                        break;
                    case "6":
                        // Exit
                        output.println("Goodbye!");
                        break;
                    default:
                        output.println("Invalid choice!");
                }

                // Close connections
                input.close();
                output.close();
                clientSocket.close();
            } catch (IOException | ParseException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        private void connectWithReducer() throws IOException, ClassNotFoundException{
            try (ServerSocket serverSocket = new ServerSocket(12348)) {
                System.out.println("Server started. Waiting for reducer...");
                Socket reducerSocket = serverSocket.accept(); // Accept reducer connection
                System.out.println("Reducer connected: " + reducerSocket);
                // Creating input stream for communication with reducer
                ObjectInputStream inputStream = new ObjectInputStream(reducerSocket.getInputStream());
                System.out.println(inputStream.readObject());
            }
        }

        // Method to connect to worker node and perform operation add accomodation
        private void insertAccomodation(String operation,Map<String, ArrayList<Map<String,String>>> Data) throws ClassNotFoundException {

            try {
                // Connect to worker node
                Socket workerNodeSocket = new Socket("localhost", 12346); // Worker node's port
                System.out.println("Connected to worker node");

                // Creating input and output streams for communication with worker node
                BufferedReader workerInput = new BufferedReader(new InputStreamReader(workerNodeSocket.getInputStream()));
                PrintWriter workerOutput = new PrintWriter(workerNodeSocket.getOutputStream(), true);

                // Creating input and output streams for communication
                ObjectOutputStream outputStream = new ObjectOutputStream(workerNodeSocket.getOutputStream());
                @SuppressWarnings("unused")
                ObjectInputStream inputStream = new ObjectInputStream(workerNodeSocket.getInputStream());

                // Send operation and data to worker node
                workerOutput.println(operation);
                outputStream.writeObject(Data);
                
                
                // Close connections
                workerInput.close();
                workerOutput.close();
                workerNodeSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Method to connect to worker node and perform operation search/rent accomodation
        @SuppressWarnings("unused")
        private void searchAccomodation(String operation, String filter, String filter2) throws ClassNotFoundException{

            try {
                // Connect to worker node
                Socket workerNodeSocket = new Socket("localhost", 12346); // Worker node's port
                System.out.println("Connected to worker node");

                // Creating input and output streams for communication with worker node
                BufferedReader workerInput = new BufferedReader(new InputStreamReader(workerNodeSocket.getInputStream()));
                PrintWriter workerOutput = new PrintWriter(workerNodeSocket.getOutputStream(), true);

                // Creating input and output streams for communication
                ObjectOutputStream outputStream = new ObjectOutputStream(workerNodeSocket.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(workerNodeSocket.getInputStream());

                // Send operation and data to worker node
                workerOutput.println(operation);
                workerOutput.println(filter);
                workerOutput.println(filter2);
                
                
                // Close connections
                workerInput.close();
                workerOutput.close();
                workerNodeSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        @SuppressWarnings("unused")
        private void rentAccomodation(String operation, String room, String beginning,String ending) throws ClassNotFoundException{

            try {
                // Connect to worker node
                Socket workerNodeSocket = new Socket("localhost", 12346); // Worker node's port
                System.out.println("Connected to worker node");

                // Creating input and output streams for communication with worker node
                BufferedReader workerInput = new BufferedReader(new InputStreamReader(workerNodeSocket.getInputStream()));
                PrintWriter workerOutput = new PrintWriter(workerNodeSocket.getOutputStream(), true);

                // Creating input and output streams for communication
                ObjectOutputStream outputStream = new ObjectOutputStream(workerNodeSocket.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(workerNodeSocket.getInputStream());

                // Send operation and data to worker node
                workerOutput.println(operation);
                workerOutput.println(room);
                workerOutput.println(beginning);
                workerOutput.println(ending);
                
                // Close connections
                workerInput.close();
                workerOutput.close();
                workerNodeSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private static Map<String, ArrayList<Map<String,String>>> parseJsonFromFile(String filePath) throws IOException, ParseException {
        // Read the content of the JSON file
        StringBuilder jsonContent = new StringBuilder();
        try (BufferedReader fileReader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = fileReader.readLine()) != null) {
                jsonContent.append(line);
            }
        }

        // Parse JSON content
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(jsonContent.toString()); // Parse as JSONObject, not JSONArray
        
        // Process JSON array and extract attributes
        Map<String, ArrayList<Map<String, String>>> result = new HashMap<>();
        for (Object ownerKey : jsonObject.keySet()) {
            ArrayList<Map<String,String>> owner_rooms= new ArrayList<>();
            JSONArray roomArray = (JSONArray) jsonObject.get(ownerKey); // Get the array of room details

            // Assuming each room has only one set of details, you can get the first element of the array
            for (Integer i=0; i<roomArray.size(); i++ ){
                JSONObject roomObject = (JSONObject) roomArray.get(i);

                // Extract individual attributes
                
                String room = (String) roomObject.get("room");
                Long noOfPersons = (Long) roomObject.get("noOfPersons");
                String area = (String) roomObject.get("area");
                Long stars = (Long) roomObject.get("stars");
                Long noOfReviews = (Long) roomObject.get("noOfReviews");
                ArrayList bookings = (ArrayList) roomObject.get("bookings");
                Long price = (Long) roomObject.get("price");
                String roomImage = (String) roomObject.get("roomImage");

                // Put attributes into the map
                Map<String, String> attributesMap = new HashMap<>();
                attributesMap.put("room", String.valueOf(room));
                attributesMap.put("noOfPersons", String.valueOf(noOfPersons));
                attributesMap.put("area", area);
                attributesMap.put("stars", String.valueOf(stars));
                attributesMap.put("noOfReviews", String.valueOf(noOfReviews));
                attributesMap.put("bookings", String.valueOf(bookings));
                attributesMap.put("price", String.valueOf(price));
                attributesMap.put("roomImage", roomImage);
                owner_rooms.add(attributesMap);

            }
            result.put((String)ownerKey, owner_rooms);



            // Add the attributes map to the result with owner as key

        }    
        return result;
    }   
}
