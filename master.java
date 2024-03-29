import java.io.*;
import java.net.*;
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

                // Send the option menu to the client
                output.println("Welcome to Booking App! What do you want to do?");

                // Read client's choice
                String clientChoice = input.readLine();

                // Ask for data
                output.println("Insert Data");

                // Read client's data (assuming it's the file path)
                String jsonDataFilePath = input.readLine();

                // Parse JSON data from the file
                Map<String, Map<String,String>> Data = parseJsonFromFile(jsonDataFilePath);

                // Process client's choice
                switch (clientChoice) {
                    case "1":
                        // Connect to worker node and perform "Add Accommodation" operation
                        connectToWorkerNodeAndPerformOperation("Add Accommodation",Data);
                        
                        //building connection with reducer
                        try (ServerSocket serverSocket = new ServerSocket(12348)) {
                            System.out.println("Server started. Waiting for reducer...");
                            Socket reducerSocket = serverSocket.accept(); // Accept reducer connection
                            System.out.println("Reducer connected: " + reducerSocket);

                            // Create input and output streams for communication with reducer
                            BufferedReader inputReducer = new BufferedReader(new InputStreamReader(reducerSocket.getInputStream()));
                            System.out.println(inputReducer);
                        }
                        break;
                    case "2":
                        // Connect to worker node and perform "Rent Accommodation" operation
                        connectToWorkerNodeAndPerformOperation("Rent Accommodation",Data);
                        //building connection with reducer
                        try (ServerSocket serverSocket = new ServerSocket(12348)) {
                            System.out.println("Server started. Waiting for reducer...");
                            Socket reducerSocket = serverSocket.accept(); // Accept reducer connection
                            System.out.println("Reducer connected: " + reducerSocket);

                            // Create input and output streams for communication with reducer
                            BufferedReader inputReducer = new BufferedReader(new InputStreamReader(reducerSocket.getInputStream()));
                            System.out.println(inputReducer);
                        }
                        break;
                    case "3":
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

        // Method to connect to worker node and perform operation
        private Map<String, Map<String,String>> connectToWorkerNodeAndPerformOperation(String operation,Map<String,Map<String,String>> Data) throws ClassNotFoundException {
            Map<String, Map<String,String>> result = new HashMap<>();

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
                outputStream.writeObject(Data);
                
                
                // Close connections
                workerInput.close();
                workerOutput.close();
                workerNodeSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return result;
        }
    }

    private static Map<String, Map<String, String>> parseJsonFromFile(String filePath) throws IOException, ParseException {
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
        Map<String, Map<String, String>> result = new HashMap<>();
        for (Object roomKey : jsonObject.keySet()) {
            JSONArray roomArray = (JSONArray) jsonObject.get(roomKey); // Get the array of room details

            // Assuming each room has only one set of details, you can get the first element of the array
            JSONObject roomObject = (JSONObject) roomArray.get(0);

            // Extract individual attributes
            String roomName = (String) roomKey;
            Long noOfPersons = (Long) roomObject.get("noOfPersons");
            String area = (String) roomObject.get("area");
            Long stars = (Long) roomObject.get("stars");
            Long noOfReviews = (Long) roomObject.get("noOfReviews");
            String roomImage = (String) roomObject.get("roomImage");

            // Put attributes into the map
            Map<String, String> attributesMap = new HashMap<>();
            attributesMap.put("noOfPersons", String.valueOf(noOfPersons));
            attributesMap.put("area", area);
            attributesMap.put("stars", String.valueOf(stars));
            attributesMap.put("noOfReviews", String.valueOf(noOfReviews));
            attributesMap.put("roomImage", roomImage);

            // Add the attributes map to the result with roomName as key
            result.put(roomName, attributesMap);
        }    
        return result;
    }   
}
