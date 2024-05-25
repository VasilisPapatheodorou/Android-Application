import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDate;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

//javac -cp json-simple-1.1.1.jar;. reducer.java
//java -cp .;json-simple-1.1.1.jar reducer

public class reducer {
    public static void main(String[] args) throws InterruptedException, ClassNotFoundException {
         // Read reducer port from configuration
        int reducerPort = Integer.parseInt(ConfigManager.getInstance().getProperty("reducerPort"));
        // Create a server socket to listen for worker connections
        try (ServerSocket serverSocket = new ServerSocket(reducerPort)) {
            System.out.println("Reducer started. Waiting for workers...");
            
            // Map to store results grouped by worker ID
            Map<Integer, Object> resultMap = new HashMap<>();
            Map<Integer, Integer> checker = new HashMap<>();

            while (true) {
                // Accept a connection from a worker
                Socket workerSocket = serverSocket.accept(); // Accept connection from worker
                System.out.println("Worker connected: " + workerSocket);

                // Create a new thread to handle the worker
                Thread workerThread = new Thread(new WorkerHandler(workerSocket, resultMap, checker));
                workerThread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class WorkerHandler implements Runnable {
        private Socket workerSocket;
        private Map<Integer, Object> resultMap;
        private Map<Integer, Integer> checker;

        public WorkerHandler(Socket workerSocket, Map<Integer, Object> resultMap, Map<Integer, Integer> checker) {
            this.workerSocket = workerSocket;
            this.resultMap = resultMap;
            this.checker = checker;
        }

        @Override
        public void run() {
            try (
                // Create input stream to read data from worker
                ObjectInputStream inputStream = new ObjectInputStream(workerSocket.getInputStream());
            ) {
                // Read the method type from the worker
                String method = (String) inputStream.readObject();
                switch (method) {
                    case "Search Accomodation":
                        handleResults(inputStream, resultMap, checker, this::aggregateSearchResults);
                        break;
                    case "Show reservations":
                        handleResults(inputStream, resultMap, checker, this::aggregateReservationResults);
                        break;
                    case "Total Output":
                        handleResults(inputStream, resultMap, checker, this::aggregateTotalOutput);
                        break;
                    default:
                        System.out.println("Unknown method: " + method);
                        break;
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        @SuppressWarnings("unchecked")
        private void handleResults(ObjectInputStream inputStream, Map<Integer, Object> resultMap, Map<Integer, Integer> checker, Aggregator aggregator) throws IOException, ClassNotFoundException {
            Map<Integer, ?> results = (Map<Integer, ?>) inputStream.readObject();
            System.out.println(results);

            synchronized (checker) {
                for (Map.Entry<Integer, ?> entry : results.entrySet()) {
                    Integer key = entry.getKey();
                    Object workerResults = entry.getValue();

                    aggregator.aggregate(resultMap, checker, key, workerResults);

                    if (isComplete(checker, key)) {
                        sendResultsToMaster(key, resultMap.get(key));
                        resultMap.remove(key);
                        checker.remove(key);
                    }
                }
            }

            workerSocket.close();
        }

        @FunctionalInterface
        private interface Aggregator {
            void aggregate(Map<Integer, Object> resultMap, Map<Integer, Integer> checker, Integer key, Object workerResults);
        }

        @SuppressWarnings("unchecked")
        private void aggregateSearchResults(Map<Integer, Object> resultMap, Map<Integer, Integer> checker, Integer key, Object workerResults) {
            if (resultMap.containsKey(key)) {
                checker.put(key, checker.get(key) + 1);
                ((ArrayList<Room>) resultMap.get(key)).addAll((ArrayList<Room>) workerResults);
            } else {
                checker.put(key, 1);
                resultMap.put(key, workerResults);
            }
        }

        @SuppressWarnings("unchecked")
        private void aggregateReservationResults(Map<Integer, Object> resultMap, Map<Integer, Integer> checker, Integer key, Object workerResults) {
            if (resultMap.containsKey(key)) {
                checker.put(key, checker.get(key) + 1);
                ((Map<String, Map<LocalDate, LocalDate>>) resultMap.get(key)).putAll((Map<String, Map<LocalDate, LocalDate>>) workerResults);
            } else {
                checker.put(key, 1);
                resultMap.put(key, workerResults);
            }
        }

        @SuppressWarnings("unchecked")
        private void aggregateTotalOutput(Map<Integer, Object> resultMap, Map<Integer, Integer> checker, Integer key, Object workerResults) {
            if (resultMap.containsKey(key)) {
                checker.put(key, checker.get(key) + 1);
                Map<String, Integer> existingResults = (Map<String, Integer>) resultMap.get(key);
                for (Map.Entry<String, Integer> resultEntry : ((Map<String, Integer>) workerResults).entrySet()) {
                    existingResults.merge(resultEntry.getKey(), resultEntry.getValue(), Integer::sum);
                }
            } else {
                checker.put(key, 1);
                resultMap.put(key, workerResults);
            }
        }

    }

    private static boolean isComplete(Map<Integer, Integer> checker, Integer key) {
        //Number of Workers
        Integer totalNodes = Integer.parseInt(ConfigManager.getInstance().getProperty("workerNodeCount"));
        // Check if results from all expected workers are received
        return checker.get(key) == totalNodes;
    }

    private static void sendResultsToMaster(Integer key, Object results) throws IOException {
        // Read master reducer port and IP from configuration
        int masterReducerPort = Integer.parseInt(ConfigManager.getInstance().getProperty("masterReducerPort"));
        String masterIp = ConfigManager.getInstance().getProperty("masterIP");
        // Connect to master node
        Socket masterSocket = new Socket(masterIp, masterReducerPort);
        System.out.println("Connected to Master");
        // Send aggregated results to master
        try (ObjectOutputStream outputMasterStream = new ObjectOutputStream(masterSocket.getOutputStream())) {
            Map<Integer, Object> finalResults = new HashMap<>();
            finalResults.put(key, results);
            outputMasterStream.writeObject(finalResults);
        }

        masterSocket.close();
    }
}
