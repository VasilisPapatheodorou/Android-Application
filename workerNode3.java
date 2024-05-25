import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class workerNode3 {
    // Shared memory
    private static ArrayList<Room> memory = new ArrayList<Room>();
    

    public static void main(String[] args) throws IOException {
        int serverPort = Integer.parseInt(ConfigManager.getInstance().getProperty("workerNodePort3"));
        @SuppressWarnings("resource")
        ServerSocket serverSocket = new ServerSocket(serverPort);
        System.out.println("Worker node started. Waiting for server...");

        while (true) {
            Socket serverConnection = serverSocket.accept(); // Accept connection from server
            System.out.println("Server connected: " + serverConnection);


            // Start a new thread to handle each client connection
            Thread requestHandlerThread = new Thread(new requestHandler(serverConnection,memory));
            requestHandlerThread.start();
        }
    }

    // requestHandler class to handle each client connection
    private static class requestHandler implements Runnable {
        private final Socket serverConnection;
        private ArrayList<Room> memory;

        public requestHandler(Socket serverConnection, ArrayList<Room> memory) {
            this.serverConnection = serverConnection;
            this.memory = memory;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            try (
                    ObjectInputStream inputStream = new ObjectInputStream(serverConnection.getInputStream());
                    ObjectOutputStream outputStream = new ObjectOutputStream(serverConnection.getOutputStream())
            ) {
                
                //read id
                Integer id = (Integer) inputStream.readObject();
                
                // Read operation from server
                String operation = (String) inputStream.readObject();
                System.out.println("Operation received: " + operation);

                //Initialize result
                //Depending on the operation we execute the appropriate code
                switch (operation) {
                    case "Add Accomodation":
                    //read person 
                    // Read the map sent by the master
                    Room room = (Room) inputStream.readObject(); //HashMap
                    
                    synchronized (memory) {
                        if(!memory.contains(room)){
                            memory.add(room);
                        }
                        // Print each Room object
                        for (Room room2 : memory) {
                            System.out.println(room2);
                        }
                    }
                    System.out.println(memory.size());

                    break;

                    case "Search Accomodation":
                        // Read the filter sent by the master
                        System.out.println("ok");
                        Map<String,Object> filters = (Map<String,Object>) inputStream.readObject();
                        // List to store filtered rooms for the current filters
                        ArrayList<Room> filteredRooms = new ArrayList<Room>();
                        System.out.println(memory);
                        synchronized(memory){
                            for (Room room2 : memory) {
                                if(!room2.getArea().equals(filters.get("area"))){
                                    System.out.println("ina");
                                    continue;
                                }
                                if (room2.getStars()<Integer.parseInt(filters.get("stars").toString())) {
                                    System.out.println("ins");
                                    continue;
                                }
                                if(room2.getNoOfPersons()<Integer.parseInt(filters.get("capacity").toString())){
                                    System.out.println("inc");
                                    continue;
                                }
                                if(room2.getPrice()>Integer.parseInt(filters.get("price").toString())){
                                    System.out.println("inp");
                                    continue;
                                }
                                String start = filters.get("start").toString();
                                String end = filters.get("end").toString();
                                if(!checkBookings(room2.getBookings(),room2.getAvailability(),start,end)){
                                    System.out.println("inb");
                                    continue;
                                }
                                filteredRooms.add(room2);           
                            }
                        }
                        // Connect to reducer
                        int reducerPort = Integer.parseInt(ConfigManager.getInstance().getProperty("reducerPort"));
                        String reducerIp = ConfigManager.getInstance().getProperty("reducerIP");
                        Socket reducerSocket = new Socket(reducerIp, reducerPort);
                        System.out.println("Connected to Reducer");

                        // Creating output stream for communication with reducer
                        ObjectOutputStream outputToReducer = new ObjectOutputStream(reducerSocket.getOutputStream());

                        // Send result to reducer
                        Map<Integer,ArrayList<Room>> final_result =new HashMap<>();
                        final_result.put(id,filteredRooms);
                        System.out.print(final_result);
                        outputToReducer.writeObject("Search Accomodation");
                        outputToReducer.writeObject(final_result);

                        // Close connections
                        reducerSocket.close();
                        break;
                    case "Rent Accomodation":
                        // Read the filter sent by the master
                        String roomChoice = (String) inputStream.readObject();
                        String startDate = (String) inputStream.readObject();

                        //Format String Date into a LocalDate Object
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
                        LocalDate start = LocalDate.parse(startDate, formatter);
                        System.out.println("Original String: " + startDate);
                        System.out.println("Parsed LocalDate: " + start);

                        String endDate = (String) inputStream.readObject();

                        //Format String Date into a LocalDate Object
                        LocalDate end = LocalDate.parse(endDate, formatter);
                        System.out.println("Original String: " + startDate);
                        System.out.println("Parsed LocalDate: " + end);
                        System.out.println("ok");

                        // Adding booking items to each room
                        
                        for (Room room2 : memory) {
                            if(room2.getRoomNumber().equals(roomChoice)){
                                synchronized(room2){
                                    room2.addBooking(start,end);
                                }
                            }
                        }
                        
                        break;
                    case "Add dates":
                        // Read the filter sent by the master
                        String managerRoomChoice = (String) inputStream.readObject();
                        String managerStartDate = (String) inputStream.readObject();

                        //Format String Date into a LocalDate Object
                        DateTimeFormatter managerFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
                        LocalDate managerStart = LocalDate.parse(managerStartDate, managerFormatter);
                        System.out.println("Original String: " + managerStartDate);
                        System.out.println("Parsed LocalDate: " + managerStart);

                        String managerEndDate = (String) inputStream.readObject();

                        //Format String Date into a LocalDate Object
                        LocalDate managerEnd = LocalDate.parse(managerEndDate, managerFormatter);
                        System.out.println("Original String: " + managerEndDate);
                        System.out.println("Parsed LocalDate: " + managerEnd);
                        System.out.println("ok");

                        // Adding booking items to each room
                        for (Room room2 : memory) {
                            if(room2.getRoomNumber().equals(managerRoomChoice)){
                                synchronized(room2){
                                    // Add managerStartDate and managerEndDate to the bookings map
                                    room2.getAvailability().put(managerStart, managerEnd);
                                }
                            }
                        }
                        System.out.println(memory);
                        break;
                        
                    case "Rate Accomodation":
                        // Read the room sent by the master
                        String roomChoice2 = (String) inputStream.readObject();
                        // Read the rating sent by the master
                        Integer rating = (Integer) inputStream.readObject();
                        for (Room room2 : memory) {
                            if(room2.getRoomNumber().equals(roomChoice2)){
                                synchronized(room2){
                                    room2.rateRoom(rating);
                                }
                            }
                        }
                        break;
                    case "Show reservations":
                        // Read the name sent by the master
                        String name = (String) inputStream.readObject();
                        Map<String,Map<LocalDate,LocalDate>> bookings=new HashMap<>();
                        System.out.println(memory);
                        for (Room room2 : memory) {
                            if(room2.getOwner().equals(name)){
                                bookings.put(room2.getRoomNumber(),room2.getBookings());
                            }
                        }
                        System.out.println(bookings);
                        // Connect to reducer
                        int reducerPort1 = Integer.parseInt(ConfigManager.getInstance().getProperty("reducerPort"));
                        String reducerIp1 = ConfigManager.getInstance().getProperty("reducerIP");
                        Socket reducerSocket1 = new Socket(reducerIp1, reducerPort1);
                        System.out.println("Connected to Reducer");

                        // Creating output stream for communication with reducer
                        ObjectOutputStream outputToReducer1 = new ObjectOutputStream(reducerSocket1.getOutputStream());

                        // Send result to reducer
                        Map<Integer,Map<String,Map<LocalDate,LocalDate>>> final_result1 =new HashMap<>();
                        final_result1.put(id,bookings);
                        outputToReducer1.writeObject("Show reservations");
                        outputToReducer1.writeObject(final_result1);

                        // Close connections
                        reducerSocket1.close();
                        break;
                    case "Total Output":
                        // Read the filter sent by the master
                        String OutputStartDate = (String) inputStream.readObject();

                        //Format String Date into a LocalDate Object
                        DateTimeFormatter managerFormatter2 = DateTimeFormatter.ofPattern("dd/MM/yyyy");
                        LocalDate OutputStart = LocalDate.parse(OutputStartDate, managerFormatter2);
                        System.out.println("Original String: " + OutputStartDate);
                        System.out.println("Parsed LocalDate: " + OutputStart);

                        String OutputEndDate = (String) inputStream.readObject();

                        //Format String Date into a LocalDate Object
                        LocalDate OutputEnd = LocalDate.parse(OutputEndDate, managerFormatter2);
                        System.out.println("Original String: " + OutputEndDate);
                        System.out.println("Parsed LocalDate: " + OutputEnd);
                        System.out.println("ok");

                        Map<String,Integer> totalbooked=new HashMap<>();

                        synchronized (memory) {
                            for (Room room2 : memory) {
                                Integer sinolo=isBooked(OutputStart,OutputEnd,room2);
                                System.out.println(sinolo);
                                if(sinolo>0){
                                    if(totalbooked.containsKey((String)room2.getArea())){
                                        totalbooked.put((String)room2.getArea(),(Integer)totalbooked.get((String)room2.getArea()) +sinolo);
                                    }else{
                                        totalbooked.put((String)room2.getArea(),sinolo);
                                    }
                                }
                            }
                        }
                        // Connect to reducer
                        int reducerPort2 = Integer.parseInt(ConfigManager.getInstance().getProperty("reducerPort"));
                        String reducerIp2 = ConfigManager.getInstance().getProperty("reducerIP");
                        Socket reducerSocket2 = new Socket(reducerIp2, reducerPort2);
                        System.out.println("Connected to Reducer");

                        // Creating output stream for communication with reducer
                        ObjectOutputStream outputToReducer2 = new ObjectOutputStream(reducerSocket2.getOutputStream());

                        // Send result to reducer
                        Map<Integer,Map<String,Integer>> final_result2 =new HashMap<>();
                        outputToReducer2.writeObject("Total Output");
                        final_result2.put(id,totalbooked);
                        
                        outputToReducer2.writeObject(final_result2);

                        // Close connections
                        reducerSocket2.close();
                        break;

                    default:
                        break;
                }
                serverConnection.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private static Integer isBooked(LocalDate beginning,LocalDate ending,Room room){
        Integer result=0;
        Map<LocalDate,LocalDate> check=(Map<LocalDate, LocalDate>) room.getBookings();

        for(Map.Entry<LocalDate,LocalDate> entry:check.entrySet()){
            System.out.println(entry.getKey());
            System.out.println(beginning);
            System.out.println(entry.getValue());
            System.out.println(ending);
            if(((entry.getKey().isAfter(beginning)||entry.getKey().isEqual(beginning)) && entry.getKey().isBefore(ending))){
                System.out.println("in");
                result+=1;
            }
        }
        return result;
    }

    private static Boolean checkBookings(Map<LocalDate,LocalDate> bookings,Map<LocalDate,LocalDate> availability,String startDate,String endDate){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
        LocalDate start = LocalDate.parse(startDate, formatter);
        LocalDate end = LocalDate.parse(endDate, formatter);;
        for(Map.Entry<LocalDate, LocalDate> reservation : bookings.entrySet()){
            
            LocalDate bookedStart = reservation.getKey();
            LocalDate bookedEnd = reservation.getValue();
            
            if((start.isBefore(bookedEnd)&&start.isAfter(bookedStart))||(end.isBefore(bookedEnd)&&end.isAfter(bookedStart))) return false;
 
        }

        for(Map.Entry<LocalDate,LocalDate> avail : availability.entrySet()){
            
            LocalDate availableStart = avail.getKey();
            LocalDate availableEnd = avail.getValue();
            if(!(start.isAfter(availableStart)&&end.isBefore(availableEnd))) return false;
        }
        
        return true;  
    }
}