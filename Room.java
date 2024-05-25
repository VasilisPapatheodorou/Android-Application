import java.io.Serializable;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class Room implements Serializable {
    private static final long serialVersionUID = 1L; // Add a unique ID for serialization
    private String owner;
    private String area;
    private Integer price;
    private String roomImage;
    private Integer stars;
    private Integer noOfPersons;
    private String roomNumber;
    private Integer noOfReviews;
    private Map<LocalDate,LocalDate> bookings;
    private Map<LocalDate,LocalDate> availability;

    // Constructor
    public Room (String owner, String area, Integer price, String roomImage, Integer noOfReviews, Integer stars, Integer noOfPersons, String roomNumber) {
        this.owner = owner;
        this.area = area;
        this.price = price;
        this.roomImage = roomImage;
        this.stars = stars;
        this.noOfPersons = noOfPersons;
        this.roomNumber = roomNumber;
        this.noOfReviews=noOfReviews;
        this.availability = new HashMap<>();
        this.bookings = new HashMap<>();
    }
    // Getters
    public String getOwner() {
        return owner;
    }

    public String getArea() {
        return area;
    }

    public Integer getPrice() {
        return price;
    }

    public String getRoomImage() {
        return roomImage;
    }

    public Integer getStars() {
        return stars;
    }

    public Integer getNoOfPersons() {
        return noOfPersons;
    }

    public String getRoomNumber() {
        return roomNumber;
    }

    public Map<LocalDate,LocalDate> getAvailability(){ return this.availability;}

    public void rateRoom(Integer rating){
        this.noOfReviews+=1;
        this.stars=(this.stars+rating)/this.noOfReviews;

    }

    public Map<LocalDate,LocalDate> getBookings(){return this.bookings;}

    public void addBooking(LocalDate start, LocalDate end){
        bookings.put(start, end);
    }

    @Override
    public String toString() {
        return "Room{" +
                "owner='" + owner + '\'' +
                ", area='" + area + '\'' +
                ", price=" + price +
                ", image='" + roomImage + '\'' +
                ", noOfReviews=" + noOfReviews +
                ", rating=" + stars +
                ", bookings="+ bookings +
                ", availability="+ availability +
                ", capacity=" + noOfPersons +
                ", roomNumber='" + roomNumber + '\'' +
                '}';
    }
}
