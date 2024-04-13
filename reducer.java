import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

public class reducer {
    public static void main(String[] args) throws InterruptedException, ClassNotFoundException {
        try (ServerSocket serverSocket = new ServerSocket(12350)) {
            System.out.println("Reducer started. Waiting for workers...");
            
            // Map to store results grouped by worker ID
            Map<Integer,Map<String, ArrayList<Map<String,Object>>>> resultMap = new HashMap<>();
            Map<Integer,Integer> checker = new HashMap<>();

            while (true) {
                Socket workerSocket = serverSocket.accept(); // Accept connection from worker
                System.out.println("Worker connected: " + workerSocket);

                // Create a new thread to handle the worker
                Thread workerThread = new Thread(new WorkerHandler(workerSocket, resultMap, checker));
                workerThread.start();
                
                
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class WorkerHandler implements Runnable {
        private Socket workerSocket;
        private Map<Integer, Map<String, ArrayList<Map<String,Object>>>> resultMap;
        private Map<Integer, Integer> checker;

        public WorkerHandler(Socket workerSocket, Map<Integer, Map<String, ArrayList<Map<String,Object>>>> resultMap, Map<Integer,Integer> checker) {
            this.workerSocket = workerSocket;
            this.resultMap = resultMap;
            this.checker = checker;
        }

        @Override
        public void run() {
            try (
                // Create input and output streams for communication with worker
                BufferedReader input = new BufferedReader(new InputStreamReader(workerSocket.getInputStream()));
                PrintWriter output = new PrintWriter(workerSocket.getOutputStream(), true);

                // Creating input stream for worker
                ObjectInputStream inputStream = new ObjectInputStream(workerSocket.getInputStream());
            ) {
                // Read the map sent by the worker
                Map<Integer,Map<String, ArrayList<Map<String,Object>>>> resultFromWorker = (Map<Integer,Map<String, ArrayList<Map<String,Object>>>>) inputStream.readObject(); //HashMap

                // Check if results for this worker ID exist
                synchronized(checker){
                    for (Map.Entry<Integer, Map<String, ArrayList<Map<String,Object>>>> Entry : resultFromWorker.entrySet()) {
                        Integer outerKey = Entry.getKey();
                        if (resultMap.containsKey(outerKey)) {
                            System.out.println("in1");
                            checker.put(outerKey, checker.get(outerKey)+1);
                            // Merge worker results with existing results for this ID
                            mergeResults(resultMap.get(outerKey), Entry.getValue());
                        } else {
                            System.out.println("in2");
                            checker.put(outerKey, 1);
                            // Create a new entry for this worker ID
                            resultMap.put(outerKey, Entry.getValue());
                        }
                        //System.out.println(resultMap);

                        // Check if all results for an ID are complete (implemented)
                        System.out.println(checker);
                        if (isComplete(checker, outerKey)) {
                            // Send complete results for this worker ID to another server (implementation needed)
                            sendResultsToServer(resultMap.get(outerKey));
                            // Optionally, remove processed results from resultMap
                            resultMap.remove(outerKey);
                            checker.remove(outerKey);
                        }
                        // Close connection
                        input.close();
                        output.close();
                        workerSocket.close();
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        
    }

    private static void mergeResults(Map<String, ArrayList<Map<String, Object>>> existingResults, Map<String, ArrayList<Map<String, Object>>> workerResults) {
        for (Map.Entry<String, ArrayList<Map<String, Object>>> entry : workerResults.entrySet()) {
            String key = entry.getKey();
            ArrayList<Map<String, Object>> value = entry.getValue();

            if (existingResults.containsKey(key)) {
                existingResults.get(key).addAll(value);
            } else {
                existingResults.put(key, value);
            }
        }
    }

    // Check if all results for a worker ID are complete (implemented)
    private static boolean isComplete(Map<Integer,Integer> checker, Integer key) {
        // Check if there's an entry for the worker ID and if it has 3 results (assuming each result is a Map)
        return checker.get(key)==3;
    }

    // Implement this method to send results to another server
    private static void sendResultsToServer( Map<String, ArrayList<Map<String, Object>>> results) throws UnknownHostException, IOException {

        // Connect to master
        Socket MasterSocket = new Socket("192.168.1.13", 12353);
        System.out.println("Connected to Master");


        // Creating output stream for master
        ObjectOutputStream outputMasterStream = new ObjectOutputStream(MasterSocket.getOutputStream());
        outputMasterStream.writeObject(results);

        // Close connections
        MasterSocket.close();
    }
}
