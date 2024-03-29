import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

//javac -cp json-simple-1.1.1.jar parseJsonFile.java
//java -cp .;json-simple-1.1.1.jar parseJsonFile

public class parseJsonFile {
    public static Map<String, Map<String, String>> parseJsonFromFile(String filePath) throws IOException, ParseException {
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
