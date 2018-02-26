/**
 * Object to hold Key-Value for Task1
 */
public class Task1JobObject {
    String KeyDeparture;  //Key
    String[] Payload = new String[7]; //Value

    public Task1JobObject() {
    }

    /**
     * Constructor for job object. Sets the keyAttribute and values from passed rows.
     * @param SingleRow
     */
    public Task1JobObject(String[] SingleRow) {
       //String[] Payload = new String[7];  //these assign the data from value to the payload of this object. row specific as key was a row.
       Payload[0]=SingleRow[0];
       Payload[1]=SingleRow[1];;
       Payload[2]=SingleRow[3];;
       Payload[3]=SingleRow[4];;
       Payload[4]=SingleRow[5];;
       Payload[5]=SingleRow[6];;
       Payload[6]=SingleRow[7];;

       KeyDeparture = SingleRow[2]; //Key assignment.
    }
}


