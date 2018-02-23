import com.sun.org.apache.regexp.internal.RE;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Arrays;

public class AdvComp {

    public static void main(String[] args) throws IOException
    {

        ArrayList<Task1JobObject> Mapper1 = new ArrayList<Task1JobObject>();
        ArrayList<Task2JobObject> Mapper2 = new ArrayList<Task2JobObject>();

        ArrayList<String[]> Data = new ArrayList<String[]>();

        HashMap<String,  ArrayList<String[]>>HMap = new HashMap<String,  ArrayList<String[]>>();
        HashMap<String,  ArrayList<String[]>>HMap2 = new HashMap<String,  ArrayList<String[]>>();

        ArrayList<String> AirPortsUsed = new ArrayList<String>();

        Data = GetDataFromCSV(Data);
        Mapper1 = TurnToKeyValuesT1(Data, Mapper1);
        HMap = CreateHashMapT1(HMap, Mapper1);

        String ListOfAirportsArray [] = {"AMS","ATL","BKK","CAN","CDG","CGK","CLT","DEN","DFW","DXB","FCO","FRA","HKG","HND","IAH","IST","JFK","KUL","LAS","LAX","LHR","MAD","MIA","MUC","ORD","PEK","PHX","PVG","SFO","SIN"};
        ArrayList<String> ListOfAirports = new ArrayList<String>(Arrays.asList(ListOfAirportsArray));

        for (String Key:HMap.keySet())
        {
            ArrayList<String[]> Value = HMap.get(Key);
            AirPortsUsed = ReduceTask1(Key, Value, AirPortsUsed);
        }

        String NotUsed = "";

        for(int i=0;i<=ListOfAirports.size()-1;i++){
            if(AirPortsUsed.contains(ListOfAirports.get(i))!=false){
//                NotUsed = NotUsed + ", "+ListOfAirports.get(i);
            }else{
                if (NotUsed == "")
                {
                    NotUsed = NotUsed + ""+ListOfAirports.get(i);
                }
                NotUsed = NotUsed + ", "+ListOfAirports.get(i);
            }
        }

        System.out.println("These airport did not get flights: "+NotUsed);
        System.out.println();


        Data = GetDataFromCSV(Data);
        Mapper2 = TurnToKeyValuesT2(Data, Mapper2);
        HMap2 = CreateHashMapT2(HMap2, Mapper2);
        for (String Key:HMap2.keySet())
        {
            ArrayList<String[]> Value = HMap2.get(Key);
            ReduceTask2(Key, Value);
        }

        for (String Key:HMap2.keySet())
        {
            ArrayList<String[]> Value = HMap2.get(Key);
            ReduceTask3(Key, Value);
        }
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

    public static ArrayList<Task1JobObject> TurnToKeyValuesT1(ArrayList<String[]> Data, ArrayList<Task1JobObject> Mapper ) throws IOException
    {
        String[] Row = new String[8];
        while (Data.isEmpty() != true)
        {
            Row = Data.get(Data.size()-1);
            Data.remove(Data.size()-1);
            Mapper.add(new Task1JobObject(Row));
        }
        return Mapper;
    }

    public static ArrayList<Task2JobObject> TurnToKeyValuesT2(ArrayList<String[]> Data, ArrayList<Task2JobObject> Mapper ) throws IOException
    {
        String[] Row = new String[8];
        while (Data.isEmpty() != true)
        {
            Row = Data.get(Data.size()-1);
            Data.remove(Data.size()-1);
            Mapper.add(new Task2JobObject(Row));
        }
        return Mapper;
    }

    public static HashMap<String,  ArrayList<String[]>> CreateHashMapT1(HashMap<String,  ArrayList<String[]>> HMap, ArrayList<Task1JobObject> Mapper ) throws IOException
    {
        Task1JobObject Single = null;

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

    public static HashMap<String,  ArrayList<String[]>> CreateHashMapT2(HashMap<String,  ArrayList<String[]>> HMap, ArrayList<Task2JobObject> Mapper ) throws IOException
    {
        Task2JobObject Single = null;

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

    public static ArrayList<String> ReduceTask1(String key, ArrayList<String[]> data, ArrayList<String> NotIncluded)
    {
//        ArrayList<String[]> Temp = new ArrayList<String[]>();
        HashMap<String,  String> ReduceHMap = new HashMap<String,  String>();

        for (int i =0; i <= data.size()-1; i++)
        {
            String TempString [] = data.get(i);
            ReduceHMap.put(TempString[1],TempString[2]);
        }

            NotIncluded.add(key);

        System.out.println("Their were "+ ReduceHMap.size()+" flights from "+ key);
        return NotIncluded;
    }

    public static void ReduceTask2(String key, ArrayList<String[]> data )
    {
        String TempStringA [] = data.get(0);
        SimpleDateFormat hhmmss;
        hhmmss = new SimpleDateFormat("hh:mm:ss");
        Date Departure = new Date(Long.parseLong(TempStringA[3]));
        Date Arrival = new Date(Long.parseLong(TempStringA[3]+ Long.parseLong(TempStringA[4])*60));
        System.out.println("Flight: "+key+". "+  TempStringA[1] + " -> "+TempStringA[2] +". Departure Time: "+ hhmmss.format(Departure) + ". Arrival Time: "+hhmmss.format(Arrival) +". Flight duration: " +TempStringA[4] + " minutes.");
        for (int i=0; i<=data.size()-1; i++)
        {
            String TempString [] = data.get(i);
            System.out.println("\t" +  TempString[0] );

        }
        System.out.println("");

    }

    public static void ReduceTask3(String key, ArrayList<String[]> data )
    {
        ArrayList<String[]> Temp = new ArrayList<String[]>();

        System.out.println( data.size()+"\t" +" on Flight -->"+"\t" + key);

    }
}