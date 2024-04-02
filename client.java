import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.ArrayList;

//javac -cp json-simple-1.1.1.jar client.java
//java -cp .;json-simple-1.1.1.jar client

public class client {
    @SuppressWarnings("unused")
    public static void main(String[] args) throws ParseException {
        try {
            Socket socket = new Socket("localhost", 12345); // Connect to localhost on port 12345

            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
            ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());

            // Send the option menu to the client
            System.out.println("Welcome to Booking App!");
            System.out.println("Choose action by number");
            System.out.println("1. Add Accommodation");
            System.out.println("2. Rent accomodation");
            System.out.println("3. Rate accomodation");
            System.out.println("4. Search Accommodation");
            System.out.println("5. Show reservations");
            System.out.println("6. Exit");

            // Read user input from the console and send it to the server
            
            String userInput1 = readUserInput();
            sendToServer(output, userInput1);

            // Read and print server response
            readAndPrintResponse(input);
            
            String userInput2 = readUserInput();

            switch (userInput1) {
                case "1":
                    outputStream.writeObject(parseJsonFromFile(userInput2));
                    break;
            
                default:
                    sendToServer(output, userInput2);
                    break;
            }

            readAndPrintResponse(input);

            String userInput3 = readUserInput();
            sendToServer(output, userInput3);

            readAndPrintResponse(input);

            String userInput4 = readUserInput();
            sendToServer(output, userInput4);

            // Receive and print server's response
            String response;
            while ((response = input.readLine()) != null) {
                System.out.println("Server: " + response);
            }

            // Close connections
            input.close();
            output.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void readAndPrintResponse(BufferedReader input) throws IOException {
        String serverResponse;
        if ((serverResponse = input.readLine()) != null) {
            System.out.println(serverResponse);
        }
    }

    private static String readUserInput() throws IOException {
        BufferedReader consoleInput = new BufferedReader(new InputStreamReader(System.in));
        return consoleInput.readLine();
    }

    private static void sendToServer(PrintWriter output, String userInput) {
        output.println(userInput);
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


