import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

public class reducer {
    public static void main(String[] args) throws InterruptedException, ClassNotFoundException {
        try {
            try (ServerSocket serverSocket = new ServerSocket(12347)) {
                System.out.println("Reducer started. Waiting for workers...");

                while (true) {
                    Socket workerSocket = serverSocket.accept(); // Accept connection from worker
                    System.out.println("Worker connected: " + workerSocket);

                    // Create input and output streams for communication with worker
                    BufferedReader input = new BufferedReader(new InputStreamReader(workerSocket.getInputStream()));
                    PrintWriter output = new PrintWriter(workerSocket.getOutputStream(), true);

                    // Creating input stream for worker
                    ObjectInputStream inputStream = new ObjectInputStream(workerSocket.getInputStream());

                    // Read the map sent by the worker
                    Map<String, Map<String,String>> resultFromWorker = (Map<String, Map<String,String>>) inputStream.readObject(); //HashMap
                    
                    // Connect to master
                    Socket MasterSocket = new Socket("localhost", 12348);
                    System.out.println("Connected to Master");


                    // Creating output stream for master
                    ObjectOutputStream outputMasterStream = new ObjectOutputStream(MasterSocket.getOutputStream());
                    outputMasterStream.writeObject(resultFromWorker);

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
}
