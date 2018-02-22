import java.io.*;
import java.util.*;
import java.util.Map;

public class AdvComp {

    public static void main(String[] args) throws IOException
    {

        ArrayList<Task1ReduceObject> Mapper1 = new ArrayList<Task1ReduceObject>();
        ArrayList<Task2ReduceObject> Mapper2 = new ArrayList<Task2ReduceObject>();
        ArrayList<String[]> Data = new ArrayList<String[]>();

        HashMap<String,  ArrayList<String[]>>HMap = new HashMap<String,  ArrayList<String[]>>();
        HashMap<String,  ArrayList<String[]>>HMap2 = new HashMap<String,  ArrayList<String[]>>();

        Data = GetDataFromCSV(Data);
        Mapper1 = TurnToKeyValuesT1(Data, Mapper1);
        HMap = CreateHashMapT1(HMap, Mapper1);
        HMap.forEach((key, value) -> PrintPayloadTast11(key, value));

        System.out.println("Debug");


        Data = GetDataFromCSV(Data);
        Mapper2 = TurnToKeyValuesT2(Data, Mapper2);
        HMap2 = CreateHashMapT2(HMap2, Mapper2);



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

    public static ArrayList<Task1ReduceObject> TurnToKeyValuesT1(ArrayList<String[]> Data, ArrayList<Task1ReduceObject> Mapper ) throws IOException
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

    public static ArrayList<Task2ReduceObject> TurnToKeyValuesT2(ArrayList<String[]> Data, ArrayList<Task2ReduceObject> Mapper ) throws IOException
    {
        String[] Row = new String[8];
        while (Data.isEmpty() != true)
        {
            Row = Data.get(Data.size()-1);
            Data.remove(Data.size()-1);
            Mapper.add(new Task2ReduceObject(Row));
        }
        return Mapper;
    }

    public static HashMap<String,  ArrayList<String[]>> CreateHashMapT1(HashMap<String,  ArrayList<String[]>> HMap, ArrayList<Task1ReduceObject> Mapper ) throws IOException
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

    public static HashMap<String,  ArrayList<String[]>> CreateHashMapT2(HashMap<String,  ArrayList<String[]>> HMap, ArrayList<Task2ReduceObject> Mapper ) throws IOException
    {
        Task2ReduceObject Single = null;

        while (Mapper.isEmpty()!=true)
        {
            Single = Mapper.get(Mapper.size()-1);
            Mapper.remove(Mapper.size()-1);

            if( HMap.size()==0)
            {
                ArrayList<String[]> Payload = new ArrayList<String[]>();
                Payload.add(Single.Payload);
                HMap.put(Single.FlightIDDeparture, Payload);
            }
            else
            {
                for (int j =0; j <= HMap.size(); j++)
                {
                    ArrayList<String[]> Payload = new ArrayList<String[]>();
                    if (HMap.containsKey(Single.FlightIDDeparture))
                    {
                        Payload = HMap.get(Single.FlightIDDeparture);
                        Payload.add(Single.Payload);
                        //HMap.get(Single.KeyDeparture);
                        HMap.put(Single.FlightIDDeparture,Payload);
                        break;
                    }
                    else
                    {
                        Payload.add(Single.Payload);
                        HMap.put(Single.FlightIDDeparture, Payload);
                        break;
                    }
                }
            }
        }
        return HMap;
    }

    public static void PrintPayloadTast11(String key, ArrayList<String[]> data )
    {
//        ArrayList<String[]> Temp = new ArrayList<String[]>();
//
//        for (int i=0; i<=ArrayList.Data(); i++ )
//        {
//            Temp = ArrayList.get(i);
            System.out.println("Their were "+ data.size()+" flights from "+ key);

    }
}