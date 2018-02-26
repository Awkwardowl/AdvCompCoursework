import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;


/**
 * Task2Reducer, Returns details on a flight as well as the passengers who were on the flight.
 */
public class Task2Reducer extends Thread{
    String key;  //store key locally.
    ArrayList<String[]> data;
    ArrayList<String> passengers = new ArrayList<>();
    String RValue = "";
    String[][] Names;

    /**
     * getter for RValue.
     * @return
     */
    public String getRValue() {
        return RValue;
    } //allows return of RValue to main.

    public ArrayList<String> getPassengers() { return passengers; } //Allows return of a list of passengers to main()

    /**
     * Constructor for Reducer Thread.
      * @param keyI
     * @param dataI
     * @param ListOfAirportNames
     */
    public Task2Reducer(String keyI, ArrayList<String[]> dataI,String[][] ListOfAirportNames ) //Constructor
    {
        key = keyI;
        data = dataI;
        Names= ListOfAirportNames;
    }



    @Override
    public void run() {
        String TempStringA [] = data.get(0); //gets the first line of data.

        SimpleDateFormat hhmmss; //DateTimeformater
        hhmmss = new SimpleDateFormat("HH:mm:ss"); //Sets the format type.

        Date Departure = new Date(Long.parseLong(TempStringA[3])*1000); //calculates the departure time from epoch time
        Date Arrival = new Date(Long.parseLong(TempStringA[3])*1000+ Long.parseLong(TempStringA[4])*60*1000); //calculates arrival time from epoch time + min coverted to epoch time.

        HashMap<String,String> PassengerCheck = new HashMap<>();

        for (int i=0; i<=data.size()-1; i++) //
        {
            String[] hold = data.get(i);
            PassengerCheck.put(hold[0],"");
        }

        for (int i=0; i <= Names[0].length-1; i++)
        {
            if (TempStringA[1].equals(Names[1][i]))
            {
               TempStringA[1] = TempStringA[1]+" (" + Names[0][i] +")"; //adds the airport name by the code
            }
        }

        for (int i=0; i <= Names[0].length-1; i++)
        {
            if (TempStringA[2].equals(Names[1][i]))
            {
                TempStringA[2] = TempStringA[2]+" (" + Names[0][i] +")"; //adds the airport name by the code
            }
        }

        RValue = RValue + PassengerCheck.size() +" on flight: "+key+". "+  TempStringA[1] + " -> "+TempStringA[2] +". Departure Time: "+ hhmmss.format(Departure) + ". Arrival Time: "+hhmmss.format(Arrival) +". Flight duration: " +TempStringA[4] + " minutes."; //writes the line with info

        for (String Key:PassengerCheck.keySet())
        {
            passengers.add(Key);
        }
        RValue=RValue + ("");
    }
}
