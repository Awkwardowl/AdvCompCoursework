/**
 * Object to hold Key-Value for Task 2 and 3.
 */
public class Task2JobObject {
    String FlightIDDeparture; //Key
    String[] Payload = new String[7]; //Value

    public Task2JobObject() {
    }

    /**
     * Constructor for job object. Sets the keyAttribute and values from passed rows.
     * @param SingleRow
     */
    public Task2JobObject(String[] SingleRow) {
       //String[] Payload = new String[7]; //these assign the data from value to the payload of this object. row specific as key was a row.
       Payload[0]=SingleRow[0];
       Payload[1]=SingleRow[2];;
       Payload[2]=SingleRow[3];;
       Payload[3]=SingleRow[4];;
       Payload[4]=SingleRow[5];;
       Payload[5]=SingleRow[6];;
       Payload[6]=SingleRow[7];;

       FlightIDDeparture = SingleRow[1]; //Key assignment
    }
}


