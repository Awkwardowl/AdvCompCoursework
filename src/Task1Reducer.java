import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class Task1Reducer extends Thread {
    String key;
    ArrayList<String[]> data;
    ArrayList<String> NotIncluded;
    String[][] Names;
    PrintWriter writer ;
    String RValue;

    public String getRValue() { return RValue; }

    public ArrayList<String> getNotIncluded() { return NotIncluded; }

    public Task1Reducer(String keyI, ArrayList<String[]> dataI, String[][] ListOfAirportNames)
    {
        key = keyI;
        data = dataI;
        Names = ListOfAirportNames;
    }

    public String getKey() {
        return key;
    }

    @Override
    public void run() {


            HashMap<String,  String> ReduceHMap = new HashMap<String,  String>();
            for (int i =0; i <= data.size()-1; i++)
            {
                String TempString [] = data.get(i);
                ReduceHMap.put(TempString[1],TempString[2]);
            }
            for (int i=0; i <= Names[0].length-1; i++)
            {
                if (key.equals(Names[1][i]))
                {
                   RValue="There were "+ ReduceHMap.size()+" flights from "+ key + " ("+Names[0][i]+")";
                }
            }
            //NotIncluded.add(key);
//            writer.println("Their were "+ ReduceHMap.size()+" flights from "+ key);

    }



}
