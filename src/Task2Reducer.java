import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class Task2Reducer extends Thread{
    String key;
    ArrayList<String[]> data;
    ArrayList<String> passengers = new ArrayList<>();
    String RValue = "";
    String[][] Names;

    public String getRValue() {
        return RValue;
    }

    public ArrayList<String> getPassengers() { return passengers; }

    public Task2Reducer(String keyI, ArrayList<String[]> dataI,String[][] ListOfAirportNames )
    {
        key = keyI;
        data = dataI;
        Names= ListOfAirportNames;
    }

    @Override
    public void run() {
        String TempStringA [] = data.get(0);

        SimpleDateFormat hhmmss;
        hhmmss = new SimpleDateFormat("HH:mm:ss");

        Date Departure = new Date(Long.parseLong(TempStringA[3])*1000);
        Date Arrival = new Date(Long.parseLong(TempStringA[3])*1000+ Long.parseLong(TempStringA[4])*60*1000);

        HashMap<String,String> PassengerCheck = new HashMap<>();

        for (int i=0; i<=data.size()-1; i++)
        {
            String[] hold = data.get(i);
            PassengerCheck.put(hold[0],"");
        }

        for (int i=0; i <= Names[0].length-1; i++)
        {
            if (TempStringA[1].equals(Names[1][i]))
            {
               TempStringA[1] = TempStringA[1]+" (" + Names[0][i] +")";
            }
        }

        for (int i=0; i <= Names[0].length-1; i++)
        {
            if (TempStringA[2].equals(Names[1][i]))
            {
                TempStringA[2] = TempStringA[2]+" (" + Names[0][i] +")";
            }
        }

        RValue = RValue + PassengerCheck.size() +" on flight: "+key+". "+  TempStringA[1] + " -> "+TempStringA[2] +". Departure Time: "+ hhmmss.format(Departure) + ". Arrival Time: "+hhmmss.format(Arrival) +". Flight duration: " +TempStringA[4] + " minutes.";

        for (String Key:PassengerCheck.keySet())
        {
            passengers.add(Key);
        }
        RValue=RValue + ("");
    }



}
