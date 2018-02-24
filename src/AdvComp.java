import com.sun.org.apache.regexp.internal.RE;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Arrays;

public class AdvComp {

    public static void main(String[] args) throws IOException, InterruptedException {

        ArrayList<Task1JobObject> Mapper1 = new ArrayList<>();
        ArrayList<Task2JobObject> Mapper2 = new ArrayList<>();

        ArrayList<String[]> Data = new ArrayList<>();

        HashMap<String,  ArrayList<String[]>>HMap = new HashMap<>();
        HashMap<String,  ArrayList<String[]>>HMap2 = new HashMap<>();

        ArrayList<String> AirPortsUsed = new ArrayList<>();

        Data = GetDataFromCSV(Data);
        ArrayList<String[]> Data2 = new ArrayList<>();
        Data2.addAll(Data);
        System.out.println("");
        List<String[]> DataA = Data.subList(0,(Data2.size()/2));
        List<String[]> DataB = Data.subList(Data2.size()/2,Data2.size());

        Task1Mapper TaskObj1 = new Task1Mapper(DataA);
        Task1Mapper TaskObj1B = new Task1Mapper(DataB);
        TaskObj1.start();
        TaskObj1B.start();
        TaskObj1.join();
        TaskObj1B.join();

        Mapper1.addAll(TaskObj1.getMapper());
        Mapper1.addAll(TaskObj1B.getMapper());

        HMap = CreateHashMapT1(HMap, Mapper1);

        String ListOfAirportsArray [] = {"AMS","ATL","BKK","CAN","CDG","CGK","CLT","DEN","DFW","DXB","FCO","FRA","HKG","HND","IAH","IST","JFK","KUL","LAS","LAX","LHR","MAD","MIA","MUC","ORD","PEK","PHX","PVG","SFO","SIN"};
        ArrayList<String> ListOfAirports = new ArrayList<>(Arrays.asList(ListOfAirportsArray));

        PrintWriter writer = new PrintWriter("ObjectiveOne.txt", "UTF-8");

        ArrayList<Task1Reducer> T1Reducers = new ArrayList<>();

        for (String Key:HMap.keySet())
        {
            ArrayList<String[]> Value = HMap.get(Key);
            Task1Reducer Reduce = new Task1Reducer(Key, Value, AirPortsUsed, writer);
            Reduce.start();
            T1Reducers.add(Reduce);
        }

        for (int i=0; i<=T1Reducers.size()-1; i++)
        {
            T1Reducers.get(i).join();
        }

        String NotUsed = "";

        for(int i=0;i<=ListOfAirports.size()-1;i++){
            if(AirPortsUsed.contains(ListOfAirports.get(i))!=false){
            }else{
                if (NotUsed == "")
                {
                    NotUsed = NotUsed + ""+ListOfAirports.get(i);
                }else{
                    NotUsed = NotUsed + ", "+ListOfAirports.get(i);
                }

            }
        }

        writer.println();
        writer.println("These airport did not get flights: "+NotUsed);
        writer.println();
        writer.close();

        PrintWriter writer2 = new PrintWriter("ObjectiveTwo.txt", "UTF-8");

        List<String[]> Data2A = Data2.subList(0,(Data2.size()/2));
        List<String[]> Data2B = Data2.subList(Data2.size()/2,Data2.size());

        Task2Mapper TaskObj2 = new Task2Mapper(Data2A);
        Task2Mapper TaskObj2B = new Task2Mapper(Data2B);
        TaskObj2.start();
        TaskObj2B.start();
        TaskObj2.join();
        TaskObj2B.join();

        Mapper2.addAll(TaskObj2.getMapper());
        Mapper2.addAll(TaskObj2B.getMapper());

        HMap2 = CreateHashMapT2(HMap2, Mapper2);
        for (String Key:HMap2.keySet())
        {
            ArrayList<String[]> Value = HMap2.get(Key);
            ReduceTask2(Key, Value, writer2);
        }
        writer2.close();

        PrintWriter writer3 = new PrintWriter("ObjectiveThree.txt", "UTF-8");

        for (String Key:HMap2.keySet())
        {
            ArrayList<String[]> Value = HMap2.get(Key);
            ReduceTask3(Key, Value,writer3);
        }
        writer3.close();
    }

    public static ArrayList<String[]> GetDataFromCSV(ArrayList<String[]> Data ) throws IOException
    {
        PrintWriter writer = new PrintWriter("ErrorLogs.txt", "UTF-8");
        int x=1;
        BufferedReader br = new BufferedReader(new FileReader("AComp_Passenger_data.csv"));
        String newLine;

        while ((newLine = br.readLine()) != null) {
        String [] array = newLine.split(",");
        if (array[0].equals("")||array[1].equals("")||array[2].equals("")||array[3].equals("")||array[4].equals("")||array[5].equals(""))
        {
            writer.println("Error: Null field found.                           Discarding row:  "+x);
            x++;
//            System.out.println("Error: Null Field Found, Discarding Row "+ x);
        }
        else
        {

            char c = array[0].charAt(0);
            if (c == '\uFEFF')
            {
               array[0]=array[0].substring(1);
            }
//            System.out.println("");
            boolean error = false;
            if (!(Character.isLetter(array[0].codePointAt(0))&&Character.isUpperCase(array[0].codePointAt(0))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x);
            }
            else if (!(Character.isLetter(array[0].codePointAt(1)) &&Character.isUpperCase(array[0].codePointAt(1))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x);


            }
            else if (!(Character.isLetter(array[0].codePointAt(2))&&Character.isUpperCase(array[0].codePointAt(2))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x);


            }
            else if (!(Character.isDigit(array[0].codePointAt(3))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x);


            }
            else if (!(Character.isDigit(array[0].codePointAt(4))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x);

            }
            else if (!(Character.isDigit(array[0].codePointAt(5))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[0]+",   Discarding row: \t"+x);

            }
            else if (!(Character.isDigit(array[0].codePointAt(6))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x);

            }
            else if (!(Character.isLetter(array[0].codePointAt(7))&&Character.isUpperCase(array[0].codePointAt(7))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x);

            }
            else if (!(Character.isLetter(array[0].codePointAt(8))&&Character.isUpperCase(array[0].codePointAt(8))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x);

            }
            else if (!(Character.isDigit(array[0].codePointAt(9))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x);

            }
            else if (!(Character.isLetter(array[2].codePointAt(0))&&Character.isUpperCase(array[2].codePointAt(0))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[2]+",           Discarding row:\t"+x);

            }
            else if (!(Character.isLetter(array[2].codePointAt(1))&&Character.isUpperCase(array[2].codePointAt(1))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[2]+",           Discarding row: \t"+x);
            }
            else if (!(Character.isLetter(array[2].codePointAt(2))&&Character.isUpperCase(array[2].codePointAt(2))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[2]+",           Discarding row: \t"+x);
            }
            else if (array[0].length() != 10 )
            {
                error = true;
                writer.println("Error: Illegal Passenger ID length: \t"+array[0]+" Discarding row: \t"+x);
            }
            else if (array[1].length() != 8 )
            {
                error = true;
                writer.println("Error: Illegal Flight ID length: \t"+array[1]+"      Discarding row: \t"+x);
            }
            else if (!(Character.isLetter(array[1].codePointAt(0))&&Character.isUpperCase(array[1].codePointAt(0))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x);
            }
            else if (!(Character.isLetter(array[1].codePointAt(1)) &&Character.isUpperCase(array[1].codePointAt(1))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x);            }
            else if (!(Character.isLetter(array[1].codePointAt(2))&&Character.isUpperCase(array[1].codePointAt(2))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x);            }
            else if (!(Character.isDigit(array[1].codePointAt(3))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x);
            }
            else if (!(Character.isDigit(array[1].codePointAt(4))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x);
            }
            else if (!(Character.isDigit(array[1].codePointAt(5))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x);
            }
            else if (!(Character.isDigit(array[1].codePointAt(6))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x);
            }
            else if (!(Character.isLetter(array[1].codePointAt(7))&&Character.isUpperCase(array[1].codePointAt(7))))
            {
                error = true;
                writer.println("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x);
            }


                String ListOfAirportsArray [] = {"AMS","ATL","BKK","CAN","CDG","CGK","CLT","DEN","DFW","DXB","FCO","FRA","HKG","HND","IAH","IST","JFK","KUL","LAS","LAX","LHR","MAD","MIA","MUC","ORD","PEK","PHX","PVG","SFO","SIN"};
                ArrayList<String> ListOfAirports = new ArrayList<String>(Arrays.asList(ListOfAirportsArray));

                for (int i=0; i<=ListOfAirportsArray.length; i++)
                {
                    if ((!(ListOfAirports.contains(array[2])))&&error==false)
                    {
                        error=true;
                        writer.println("Error: Illegal airport detected: \t"+array[2]+"            Discarding row: \t"+x);
                        break;
                    }
                }

                for (int i=0; i<=ListOfAirportsArray.length; i++)
                {
                    if ((!(ListOfAirports.contains(array[3])))&&error==false)
                    {
                        error=true;
                        writer.println("Error: Illegal airport detected: \t"+array[3]+"            Discarding row: \t"+x);
                        break;
                    }
                }




            String[] Row = new String[8];
            Row[0]=array[0];
            Row[1]=array[1];
            Row[2]=array[2];
            Row[3]=array[3];
            Row[4]=array[4];
            Row[5]=array[5];

            if (error == false)
            {
                Data.add(Row);
            }
            x++;

        }

        }
        br.close();
        writer.close();
        //System.out.println("");
        return Data;
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

//    public static ArrayList<String> ReduceTask1(String key, ArrayList<String[]> data, ArrayList<String> NotIncluded,PrintWriter writer)
//    {
////        ArrayList<String[]> Temp = new ArrayList<String[]>();
//        HashMap<String,  String> ReduceHMap = new HashMap<String,  String>();
//
//        for (int i =0; i <= data.size()-1; i++)
//        {
//            String TempString [] = data.get(i);
//            ReduceHMap.put(TempString[1],TempString[2]);
//        }
//
//            NotIncluded.add(key);
//
//        writer.println("Their were "+ ReduceHMap.size()+" flights from "+ key);
//        return NotIncluded;
//    }

    public static void ReduceTask2(String key, ArrayList<String[]> data,PrintWriter writer3 )
    {
        String TempStringA [] = data.get(0);
        SimpleDateFormat hhmmss;
        //hhmmss = new SimpleDateFormat("hh:mm:ss");
        Date Departure = new Date(Long.parseLong(TempStringA[3])*1000);
        Date Arrival = new Date(Long.parseLong(TempStringA[3])*1000+ Long.parseLong(TempStringA[4])*60*1000);

        HashMap<String,String> PassengerCheck = new HashMap<String,String>();

        for (int i=0; i<=data.size()-1; i++)
        {
            String[] hold = data.get(i);
            PassengerCheck.put(hold[0],"");
        }

//        System.out.println(data.size() +" Flight: "+key+". "+  TempStringA[1] + " -> "+TempStringA[2] +". Departure Time: "+ hhmmss.format(Departure) + ". Arrival Time: "+hhmmss.format(Arrival) +". Flight duration: " +TempStringA[4] + " minutes.");
        writer3.println(PassengerCheck.size() +" Flight: "+key+". "+  TempStringA[1] + " -> "+TempStringA[2] +". Departure Time: "+ (Departure) + ". Arrival Time: "+(Arrival) +". Flight duration: " +TempStringA[4] + " minutes.");


            for (String Key:PassengerCheck.keySet())
            {
                writer3.println("\t" +  Key );
            }

        writer3.println("");

    }

    public static void ReduceTask3(String key, ArrayList<String[]> data, PrintWriter writer2 )
    {
        ArrayList<String[]> Temp = new ArrayList<String[]>();
        HashMap<String,String> PassengerCheck = new HashMap<String,String>();

        for (int i=0; i<=data.size()-1; i++)
        {
            String[] hold = data.get(i);
            PassengerCheck.put(hold[0],"");
        }

        writer2.println( PassengerCheck.size()+"\tpassengers on flightID -->"+"\t" + key);

    }
}