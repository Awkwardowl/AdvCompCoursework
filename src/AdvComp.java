import java.io.*;
import java.util.*;

public class AdvComp {

    public static void main(String[] args) throws IOException
    {

        ArrayList<Task1ReduceObject> Mapper = new ArrayList<Task1ReduceObject>();
        ArrayList<String[]> Data = new ArrayList<String[]>();

        HashMap<String,  ArrayList<String[]>>HMap = new HashMap<String,  ArrayList<String[]>>();

        Data = GetDataFromCSV(Data);
        Mapper = TurnToKeyValues(Data, Mapper);
        HMap = CreateHashMap(HMap, Mapper);

        System.out.println("Debug");
    }

    public static ArrayList<String[]> GetDataFromCSV(ArrayList<String[]> Data ) throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader("AComp_Passenger_data_no_error.csv"));
        String newLine;
        int x=0;

        while ((newLine = br.readLine()) != null)
        {

            Scanner scanner = new Scanner(newLine);
            scanner.useDelimiter(",");

            while(scanner.hasNext())
            {
                String[] Row = new String[8];
                for (int y=0; y < 8; y++)
                {
                    Row[y]= scanner.next();
                }
                Data.add(Row);
            }
            scanner.close();
            x++;
        }
        br.close();
        return Data;
    }

    public static ArrayList<Task1ReduceObject> TurnToKeyValues(ArrayList<String[]> Data, ArrayList<Task1ReduceObject> Mapper ) throws IOException
    {
        String[] Row = new String[8];
        while (Data.isEmpty() != true)
        {
            Row = Data.get(Data.size()-1);
            Data.remove(Data.size()-1);
            Mapper.add(new Task1ReduceObject(Row));
        }
        return Mapper;
    }
    public static HashMap<String,  ArrayList<String[]>> CreateHashMap(HashMap<String,  ArrayList<String[]>> HMap, ArrayList<Task1ReduceObject> Mapper ) throws IOException
    {
        Task1ReduceObject Single = null;

        while (Mapper.isEmpty()!=true)
        {
            Single = Mapper.get(Mapper.size()-1);
            Mapper.remove(Mapper.size()-1);

            if( HMap.size()==0)
            {
                ArrayList<String[]> Payload = new ArrayList<String[]>();
                Payload.add(Single.Payload);
                HMap.put(Single.KeyDeparture, Payload);
            }
            else
            {
                for (int j =0; j <= HMap.size(); j++)
                {
                    ArrayList<String[]> Payload = new ArrayList<String[]>();
                    if (HMap.containsKey(Single.KeyDeparture))
                    {
                        Payload = HMap.get(Single.KeyDeparture);
                        Payload.add(Single.Payload);
                        //HMap.get(Single.KeyDeparture);
                        HMap.put(Single.KeyDeparture,Payload);
                        break;
                    }
                    else
                    {
                        Payload.add(Single.Payload);
                        HMap.put(Single.KeyDeparture, Payload);
                        break;
                    }
                }
            }
        }
        return HMap;
    }

}