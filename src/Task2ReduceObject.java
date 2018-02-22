public class Task2ReduceObject {
    String FlightIDDeparture;
    String[] Payload = new String[7];

    public Task2ReduceObject() {
    }

    public Task2ReduceObject(String[] SingleRow) {
       //String[] Payload = new String[7];
       Payload[0]=SingleRow[0];
       Payload[1]=SingleRow[2];;
       Payload[2]=SingleRow[3];;
       Payload[3]=SingleRow[4];;
       Payload[4]=SingleRow[5];;
       Payload[5]=SingleRow[6];;
       Payload[6]=SingleRow[7];;

       FlightIDDeparture = SingleRow[1];
    }
}


