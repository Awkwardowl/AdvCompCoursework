import com.sun.org.apache.regexp.internal.RE;

import java.io.*;
import java.util.*;

public class AdvComp {

    public static void main(String[] args) throws IOException
    {

        ArrayList<Task1JobObject> Mapper1 = new ArrayList<Task1JobObject>();
        ArrayList<Task2JobObject> Mapper2 = new ArrayList<Task2JobObject>();

        ArrayList<String [][]> T1Results = new ArrayList<String[][]>();

        ArrayList<String[]> Data = new ArrayList<String[]>();

        HashMap<String,  ArrayList<String[]>>HMap = new HashMap<String,  ArrayList<String[]>>();
        HashMap<String,  ArrayList<String[]>>HMap2 = new HashMap<String,  ArrayList<String[]>>();

        String NotIncluded = null;

        Data = GetDataFromCSV(Data);
        Mapper1 = TurnToKeyValuesT1(Data, Mapper1);
        HMap = CreateHashMapT1(HMap, Mapper1);
        HMap.forEach((key, value) -> NotIncluded = ReduceTask1(key, value, NotIncluded));
        System.out.println("These airport did not get flights: "+NotIncluded);


        Data = GetDataFromCSV(Data);
        Mapper2 = TurnToKeyValuesT2(Data, Mapper2);
        HMap2 = CreateHashMapT2(HMap2, Mapper2);
        HMap2.forEach((key, value) -> PrintPayloadTask3(key, value));


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

    public static String ReduceTask1(String key, ArrayList<String[]> data, String NotIncluded)
    {
        ArrayList<String[]> Temp = new ArrayList<String[]>();
        HashMap<String,  String> ReduceHMap = new HashMap<String,  String>();

        for (int i =0; i <= data.size()-1; i++)
        {
            String TempString [] = data.get(i);
            ReduceHMap.put(TempString[1],TempString[2]);
        }

        String ListOfAirports [] = {"AMS","ATL","BKK","CAN","CDG","CGK","CLT","DEN","DFW","DXB","FCO","FRA","HKG","HND","IAH","IST","JKF","KUL","LAS","LAX","LHR","MAD","MIA","MUC","ORD","PEK","PHX","PVG","SFO","SIN"};

        for (int i =0; i <= ReduceHMap.size()-1; i++)
        {
            for (int j=0;j<=ListOfAirports.length-1; j++)
            if (ReduceHMap.containsKey(ListOfAirports[j])){
                NotIncluded = NotIncluded + "," + " " + ListOfAirports[j];
            }
        }
        System.out.println("Their were "+ ReduceHMap.size()+" flights from "+ key);
        return NotIncluded;
    }

    public static void PrintPayloadTask3(String key, ArrayList<String[]> data )
    {
//        ArrayList<String[]> Temp = new ArrayList<String[]>();
//
//        for (int i=0; i<=ArrayList.Data(); i++ )
//        {
//            Temp = ArrayList.get(i);
        System.out.println("Their were "+ data.size()+" passengers on flight "+ key);

    }
}