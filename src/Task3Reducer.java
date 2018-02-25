
import java.util.ArrayList;
import java.util.HashMap;

public class Task3Reducer extends Thread {
    String key;
    ArrayList<String[]> data;
    String RValue = "";

    public String getRValue() { return RValue;  }

    public Task3Reducer(String key, ArrayList<String[]> data) {
        this.key = key;
        this.data = data;
    }

    @Override
    public void run() {

        ArrayList<String[]> Temp = new ArrayList<String[]>();
        HashMap<String,String> PassengerCheck = new HashMap<String,String>();

        for (int i=0; i<=data.size()-1; i++)
        {
            String[] hold = data.get(i);
            PassengerCheck.put(hold[0],"");
        }

        RValue = RValue + PassengerCheck.size()+"\tpassengers on flightID -->"+"\t" + key;

    }
}
