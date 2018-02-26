import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This a Task1Reducer. it outputs a list of airports and the number of flights from them as well as airports not used.
 */
public class Task1Reducer extends Thread {
    String key;
    ArrayList<String[]> data;
    ArrayList<String> NotIncluded;
    String[][] Names;
    PrintWriter writer ;
    String RValue;

    /**
     * getter for Rvalue.
     * @return
     */
    public String getRValue() { return RValue; }

    /**
     * getter for notIncluded.
     * @return
     */
    public ArrayList<String> getNotIncluded() { return NotIncluded; }

    /**
     * Constructor for Reducer Thread.
     * @param keyI
     * @param dataI
     * @param ListOfAirportNames
     */
    public Task1Reducer(String keyI, ArrayList<String[]> dataI, String[][] ListOfAirportNames)
    {
        key = keyI;
        data = dataI;
        Names = ListOfAirportNames;
    }

    /**
     * getter for key.
     * @return
     */
    public String getKey() {
        return key;
    }

    @Override
    public void run() {


            HashMap<String,  String> ReduceHMap = new HashMap<String,  String>();
            for (int i =0; i <= data.size()-1; i++)
            {
                String TempString [] = data.get(i);
                ReduceHMap.put(TempString[1],TempString[2]); //removes duplicate passengers.
            }
            for (int i=0; i <= Names[0].length-1; i++)
            {
                if (key.equals(Names[1][i]))
                {
                   RValue="There were "+ ReduceHMap.size()+" flights from "+ key + " ("+Names[0][i]+")"; //for every key output the number of flights
                }
            }
            //NotIncluded.add(key);
//            writer.println("Their were "+ ReduceHMap.size()+" flights from "+ key);

    }



}
