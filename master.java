import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Map;

public class master {
    private static final String[] WORKER_NODE_IPS = {"192.168.1.13", "192.168.1.13","192.168.1.13"}; // IPs of worker nodes
    private static final int[] WORKER_NODE_PORTS = {12355, 12347,12348}; // Ports of worker nodes
    private static Integer id=0; 
    public static void main(String[] args) throws IOException { 
        
        try {

            try (ServerSocket serverSocket = new ServerSocket(12345)) {
                System.out.println("Server started. Waiting for a client...");

                while (true) {
                    Socket clientSocket = serverSocket.accept(); // Accept client connection
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

        @SuppressWarnings("unused")
        @Override
        public void run() {
            try {
                // Creating input and output streams for communication
                BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());

                // Read client's choice
                String clientChoice = input.readLine();
                System.out.println(clientChoice);
                System.out.println("ok");
                // Process client's choice
                switch (clientChoice) {
                    case "Add Accomodation":
                        // Ask for data to add accomodation
                        output.println("Insert Data");
                        // Read client's data 
                        Map<String, ArrayList<Map<String,String>>> Data = (Map<String, ArrayList<Map<String,String>>>) inputStream.readObject();

                        //Decide on wich workerNode every room will be stored

                        // Iterate over each entry in the map
                        for (Map.Entry<String, ArrayList<Map<String, String>>> entry : Data.entrySet()) {
                            String person = entry.getKey();
                            ArrayList<Map<String, String>> rooms = entry.getValue();

                            // Iterate over each item in the list
                            for (Map<String, String> roomData : rooms) {
                                // Access the "room" key of the roomData map
                                String roomName = roomData.get("room");
                                int node = Hash(roomName, WORKER_NODE_IPS.length);
                                System.out.println(node);
                                // Connect to worker node and perform "Add Accommodation" operation
                                insertAccomodation("Add Accomodation",roomData,node,person);
                            }
                        }
                        //build connection with reducer
                        //connectWithReducer();
                        break;
                    case "Rent Accomodation":

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
                        rentAccomodation("Rent Accomodation",choice,beginning,ending,id);
                        //building connection with reducer
                        result = connectWithReducer();
                        output.println(result.toString());
                        break;
                    case "Rate accomodation":
                        output.println("Rate accomodation");
                        break;
                    case "Search Accomodation":
                        //8a mporouse na pairnei san eisodo mia Map me ola ta filtra
                        // Ask for data
                        output.println("Choose filter:");
                        // Read client filter
                        String filter = input.readLine();
                        // Ask for data
                        output.println("Insert "+filter+" of choice:");
                        String filter2 = input.readLine();
                        // Connect to worker node and perform "Search Accommodation" operation
                        searchAccomodation("Search Accomodation",filter,filter2,id);
                        //building connection with reducer
                        result = connectWithReducer();
                        output.println(result.toString());
                        break;
                    case "Show reservations":
                        // Ask for data
                        output.println("Insert Name:");
                        // Read client filter
                        String name = input.readLine();
                        // Connect to worker node and perform "Show reservations" operation
                        showReservations("Show reservations",name,id);
                        //building connection with reducer
                        result = connectWithReducer();
                        output.println(result.toString());
                        break;
                    case "Add dates":
                        // Ask for room to rent
                        output.println("Choose room");
                        // Read client's choice
                        String choice2 = input.readLine();
                        // Ask for date
                        output.println("Insert beginning of availability");
                        String beginning1 = input.readLine();
                        output.println("Insert ending of availability");
                        String ending1 = input.readLine();
                        // Connect to worker node and perform "Add dates" operation
                        addDates("Add dates",choice2,beginning1,ending1,id);
                        break;
                    case "Exit":
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

        private void showReservations(String operation, String name, Integer id) {
            try {
                // Connect to worker node
                for(int i=0; i<WORKER_NODE_IPS.length; i++){
                    Socket workerNodeSocket = new Socket(WORKER_NODE_IPS[i], WORKER_NODE_PORTS[i]); // Worker node's port
                    System.out.println("Connected to worker node with IP: "+WORKER_NODE_IPS[i]+" and port "+WORKER_NODE_PORTS[i]);

                    // Creating input and output streams for communication with worker node
                    BufferedReader workerInput = new BufferedReader(new InputStreamReader(workerNodeSocket.getInputStream()));
                    PrintWriter workerOutput = new PrintWriter(workerNodeSocket.getOutputStream(), true);

                    // Creating input and output streams for communication
                    ObjectOutputStream outputStream = new ObjectOutputStream(workerNodeSocket.getOutputStream());
                    ObjectInputStream inputStream = new ObjectInputStream(workerNodeSocket.getInputStream());

                    // Send operation and data to worker node
                    outputStream.writeObject(id);
                    outputStream.writeObject(operation);
                    outputStream.writeObject(name);
        
                    // Close connections
                    workerInput.close();
                    workerOutput.close();
                    workerNodeSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private Object connectWithReducer() throws IOException, ClassNotFoundException{
            try (ServerSocket serverSocket = new ServerSocket(12353)) {
                System.out.println("Server started. Waiting for reducer...");
                Socket reducerSocket = serverSocket.accept(); // Accept reducer connection
                System.out.println("Reducer connected: " + reducerSocket);
                // Creating input stream for communication with reducer
                ObjectInputStream inputStream = new ObjectInputStream(reducerSocket.getInputStream());
                return inputStream.readObject();
            }
        }

        // Method to connect to worker node and perform operation add accomodation
        private void insertAccomodation(String operation,Map<String, String> Data, Integer node, String person) throws ClassNotFoundException {

            try {

                String workerIP = WORKER_NODE_IPS[node];
                int workerPort = WORKER_NODE_PORTS[node];
                // Connect to workerNode to insert data
                Socket workerNodeSocket = new Socket(workerIP, workerPort); // Worker node's port
                System.out.println("Connected to worker node with IP: "+workerIP+" and port "+workerPort);

                // Creating input and output streams for communication with worker node
                BufferedReader workerInput = new BufferedReader(new InputStreamReader(workerNodeSocket.getInputStream()));
                PrintWriter workerOutput = new PrintWriter(workerNodeSocket.getOutputStream(), true);

                // Creating input and output streams for communication
                ObjectOutputStream outputStream = new ObjectOutputStream(workerNodeSocket.getOutputStream());
                @SuppressWarnings("unused")
                ObjectInputStream inputStream = new ObjectInputStream(workerNodeSocket.getInputStream());

                // Send operation and data to worker node
                outputStream.writeObject(id);
                outputStream.writeObject(operation);
                outputStream.writeObject(person);
                System.out.println(Data);
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
        private void searchAccomodation(String operation, String filter, String filter2,Integer id) throws ClassNotFoundException{

            try {
                // Connect to worker node
                for(int i=0; i<WORKER_NODE_IPS.length; i++){
                    Socket workerNodeSocket = new Socket(WORKER_NODE_IPS[i], WORKER_NODE_PORTS[i]); // Worker node's port
                    System.out.println("Connected to worker node with IP: "+WORKER_NODE_IPS[i]+" and port "+WORKER_NODE_PORTS[i]);

                    // Creating input and output streams for communication with worker node
                    BufferedReader workerInput = new BufferedReader(new InputStreamReader(workerNodeSocket.getInputStream()));
                    PrintWriter workerOutput = new PrintWriter(workerNodeSocket.getOutputStream(), true);

                    // Creating input and output streams for communication
                    ObjectOutputStream outputStream = new ObjectOutputStream(workerNodeSocket.getOutputStream());
                    ObjectInputStream inputStream = new ObjectInputStream(workerNodeSocket.getInputStream());

                    // Send operation and data to worker node
                    outputStream.writeObject(id);
                    outputStream.writeObject(operation);
                    outputStream.writeObject(filter);
                    outputStream.writeObject(filter2);
                    
        
                    // Close connections
                    workerInput.close();
                    workerOutput.close();
                    workerNodeSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        @SuppressWarnings("unused")
        private void rentAccomodation(String operation, String room, String beginning,String ending,Integer id) throws ClassNotFoundException{

            try {
                // Connect to worker node
                for(int i=0; i<WORKER_NODE_IPS.length; i++){
                    Socket workerNodeSocket = new Socket(WORKER_NODE_IPS[i], WORKER_NODE_PORTS[i]); // Worker node's port
                    System.out.println("Connected to worker node with IP: "+WORKER_NODE_IPS[i]+" and port "+WORKER_NODE_PORTS[i]);


                    // Creating input and output streams for communication with worker node
                    BufferedReader workerInput = new BufferedReader(new InputStreamReader(workerNodeSocket.getInputStream()));
                    PrintWriter workerOutput = new PrintWriter(workerNodeSocket.getOutputStream(), true);

                    // Creating input and output streams for communication
                    ObjectOutputStream outputStream = new ObjectOutputStream(workerNodeSocket.getOutputStream());
                    ObjectInputStream inputStream = new ObjectInputStream(workerNodeSocket.getInputStream());

                    // Send operation and data to worker node
                    outputStream.writeObject(id);
                    outputStream.writeObject(operation);
                    outputStream.writeObject(room);
                    outputStream.writeObject(beginning);
                    outputStream.writeObject(ending);
                    
                    
                    // Close connections
                    workerInput.close();
                    workerOutput.close();
                    workerNodeSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        private static int Hash(String roomName, int numberOfNodes) {
            // Calculate the hash code for the roomName
            int hashCode = roomName.hashCode();
            // Calculate the modulo operation with numberOfNodes
            return Math.abs(hashCode) % (numberOfNodes);
        }

        @SuppressWarnings("unused")
        private void addDates(String operation, String room, String beginning,String ending,Integer id) throws ClassNotFoundException{

            try {
                // Connect to worker node
                Integer i = Hash(room, WORKER_NODE_IPS.length);
                Socket workerNodeSocket = new Socket(WORKER_NODE_IPS[i], WORKER_NODE_PORTS[i]); // Worker node's port
                System.out.println("Connected to worker node with IP: "+WORKER_NODE_IPS[i]+" and port "+WORKER_NODE_PORTS[i]);


                // Creating input and output streams for communication with worker node
                BufferedReader workerInput = new BufferedReader(new InputStreamReader(workerNodeSocket.getInputStream()));
                PrintWriter workerOutput = new PrintWriter(workerNodeSocket.getOutputStream(), true);

                // Creating input and output streams for communication
                ObjectOutputStream outputStream = new ObjectOutputStream(workerNodeSocket.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(workerNodeSocket.getInputStream());

                // Send operation and data to worker node
                outputStream.writeObject(id);
                outputStream.writeObject(operation);
                outputStream.writeObject(room);
                outputStream.writeObject(beginning);
                outputStream.writeObject(ending);
                
                
                // Close connections
                workerInput.close();
                workerOutput.close();
                workerNodeSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
            
    }
 
}
