import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;



public class master {
    public static void main(String[] args) throws IOException {

        // Create a Random object
        Random random = new Random();
        // Generate a random integer between 2 and 4 to initialize the workers
        int NumberofWorkers = random.nextInt(3) + 2;
        //List of nodes
        ArrayList<Process> nodeList = new ArrayList<Process>();
        
        //Initialize workers
        for(Integer i=0; i<=NumberofWorkers; i++){
            String command = "cmd /c start cmd.exe /K cd C:\\Users\\Bill\\Documents\\GitHub\\Android-Application && java workerNode";
            // Execute command (e.g., run a Java file)
            Process process = Runtime.getRuntime().exec(command);
            nodeList.add(process);
            System.out.println("worker "+process+" is running");
        }
        System.out.println(nodeList);
        

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

        @SuppressWarnings("unused")
        @Override
        public void run() {
            try {
                // Creating input and output streams for communication
                BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
                @SuppressWarnings("unused")
                ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());

                // Read client's choice
                String clientChoice = input.readLine();

                // Process client's choice
                switch (clientChoice) {
                    case "1":
                        // Ask for data to add accomodation
                        output.println("Insert Data");
                        // Read client's data 
                        @SuppressWarnings("unchecked") Map<String, ArrayList<Map<String,String>>> Data = (Map<String, ArrayList<Map<String,String>>>) inputStream.readObject();
                        
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
            } catch (IOException | ClassNotFoundException e) {
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
        private static int Hash(String roomName, int numberOfNodes) {
            // Calculate the hash code for the roomName
            int hashCode = roomName.hashCode();
            // Calculate the modulo operation with numberOfNodes
            return Math.abs(hashCode) % numberOfNodes;
        }
            
    }
 
}
