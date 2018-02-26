
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Task3Reducer, returns the number of passengers on a flightID.
 */
public class Task3Reducer extends Thread {
    String key;
    ArrayList<String[]> data;
    String RValue = "";

    /**
     * Getter for RValue.
     * @return
     */
    public String getRValue() { return RValue;  }

    /**
     * Constructor for task3 reducer.
     * @param key
     * @param data
     */
    public Task3Reducer(String key, ArrayList<String[]> data) {
        this.key = key;
        this.data = data;
    }

    @Override
    public void run() {

        ArrayList<String[]> Temp = new ArrayList<String[]>();
        HashMap<String,String> PassengerCheck = new HashMap<String,String>(); //used to remove duplicate passengers

        for (int i=0; i<=data.size()-1; i++)
        {
            String[] hold = data.get(i);
            PassengerCheck.put(hold[0],""); //removes duplicate passengers.
        }

        RValue = RValue + PassengerCheck.size()+"\tpassengers on flightID -->"+"\t" + key; //calculates output string.

    }
}
