public class Task1ReduceObject {
    String PrimaryKeyDeparture;

    public Task1ReduceObject(String PassID, String FlightID, String Destination, String DepartTimeEpo, String FlightTime, String ArrivalTime, String DepartTime) {
       String[] Payload = new String[7];
       Payload[0]=PassID;
       Payload[1]=FlightID;
       Payload[2]=Destination;
       Payload[3]=DepartTimeEpo;
       Payload[4]=FlightTime;
       Payload[5]=ArrivalTime;
       Payload[6]=DepartTime;
    }
}

//PassengerID
//FlightID
//DepartAirport *****
//DestinationAirport
//DepartureTimeEpo
//FlightTime
//ArrivalTime
//DepartureTime

