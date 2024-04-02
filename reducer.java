import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

public class reducer {
    public static void main(String[] args) throws InterruptedException, ClassNotFoundException {
        try {
            try (ServerSocket serverSocket = new ServerSocket(12347)) {
                System.out.println("Reducer started. Waiting for workers...");
                
                // Create a map to accumulate results from each worker
                Map<String, ArrayList<Map<String,String>>> aggregatedResult = new HashMap<>();

                while (true) {
                    Socket workerSocket = serverSocket.accept(); // Accept connection from worker
                    System.out.println("Worker connected: " + workerSocket);

                    // Create input and output streams for communication with worker
                    BufferedReader input = new BufferedReader(new InputStreamReader(workerSocket.getInputStream()));
                    PrintWriter output = new PrintWriter(workerSocket.getOutputStream(), true);

                    // Creating input stream for worker
                    ObjectInputStream inputStream = new ObjectInputStream(workerSocket.getInputStream());

                    // Read the map sent by the worker
                    @SuppressWarnings("unchecked")
                    Map<String, ArrayList<Map<String,String>>> resultFromWorker = (Map<String, ArrayList<Map<String,String>>>) inputStream.readObject(); //HashMap
                    
                    // Aggregate the result from this worker
                    aggregateResult(aggregatedResult, resultFromWorker);
                    
                    // Connect to master
                    Socket MasterSocket = new Socket("localhost", 12348);
                    System.out.println("Connected to Master");


                    // Creating output stream for master
                    ObjectOutputStream outputMasterStream = new ObjectOutputStream(MasterSocket.getOutputStream());
                    outputMasterStream.writeObject(aggregatedResult);

                    // Close connections
                    MasterSocket.close();
                    input.close();
                    output.close();
                    workerSocket.close();
   
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void aggregateResult(Map<String, ArrayList<Map<String,String>>> aggregatedResult, Map<String, ArrayList<Map<String,String>>> resultFromWorker) {
        // Iterate over the entries in the resultFromWorker and add them to the aggregatedResult
        for (Map.Entry<String, ArrayList<Map<String,String>>> entry : resultFromWorker.entrySet()) {
            String key = entry.getKey();
            ArrayList<Map<String,String>> value = entry.getValue();

            // If the key already exists in aggregatedResult, merge the ArrayLists
            if (aggregatedResult.containsKey(key)) {
                aggregatedResult.get(key).addAll(value);
            } else {
                // If the key doesn't exist, add a new entry to aggregatedResult
                aggregatedResult.put(key, value);
            }
        }
    }
}
