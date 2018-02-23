public class Task1JobObject {
    String KeyDeparture;
    String[] Payload = new String[7];

    public Task1JobObject() {
    }

    public Task1JobObject(String[] SingleRow) {
       //String[] Payload = new String[7];
       Payload[0]=SingleRow[0];
       Payload[1]=SingleRow[1];;
       Payload[2]=SingleRow[3];;
       Payload[3]=SingleRow[4];;
       Payload[4]=SingleRow[5];;
       Payload[5]=SingleRow[6];;
       Payload[6]=SingleRow[7];;

       KeyDeparture = SingleRow[2];
    }
}


