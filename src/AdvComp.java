import java.io.*;
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

        String ListOfAirportsArray [][] = getAirports();
        String ListOfAirportsCode[] = ListOfAirportsArray[1];
        String ListOfAirportsName[] = ListOfAirportsArray[0];

        Data = GetDataFromCSV(Data,ListOfAirportsCode);

        ArrayList<String[]> Data2 = new ArrayList<>();

        Data2.addAll(Data);

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

        HMap = Task1Shuffler(HMap, Mapper1);

        ArrayList<String> ListOfAirports = new ArrayList<>(Arrays.asList(ListOfAirportsCode));
        ArrayList<String> ListOfAirportsNames = new ArrayList<>(Arrays.asList(ListOfAirportsName));

        PrintWriter writer = new PrintWriter("ObjectiveOne.txt", "UTF-8");

        ArrayList<Task1Reducer> T1Reducers = new ArrayList<>();

        for (String Key:HMap.keySet())
        {
            ArrayList<String[]> Value = HMap.get(Key);
            Task1Reducer Reduce = new Task1Reducer(Key, Value, AirPortsUsed, writer,ListOfAirportsArray);
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
                    NotUsed = NotUsed + ListOfAirports.get(i)+"("+ListOfAirportsNames.get(i)+")";
                }else{
                    NotUsed = NotUsed + ", " +ListOfAirports.get(i)+"("+ListOfAirportsNames.get(i)+")";
                }
            }
        }

        writer.println();
        writer.println("These airport did not get flights: "+NotUsed);
        writer.println();
        writer.close();

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

        HMap2 = Task2Shuffler(HMap2, Mapper2);

        ArrayList<Task2Reducer> T2Reducers = new ArrayList<>();

        PrintWriter writer2 = new PrintWriter("ObjectiveTwo.txt", "UTF-8");

        for (String Key:HMap2.keySet())
        {
            ArrayList<String[]> Value = HMap2.get(Key);
            Task2Reducer Reduce2 = new Task2Reducer(Key, Value,ListOfAirportsArray);
            Reduce2.start();
            T2Reducers.add(Reduce2);
        }

        for (int i=0; i<=T2Reducers.size()-1; i++)
        {
            T2Reducers.get(i).join();

            writer2.println(T2Reducers.get(i).getRValue());
            ArrayList<String> passengers=T2Reducers.get(i).passengers;
            for (int j=0; j<=passengers.size()-1; j++)
            {
                writer2.println(passengers.get(j));
            }
            writer2.println("");

        }

        writer2.close();

        PrintWriter writer3 = new PrintWriter("ObjectiveThree.txt", "UTF-8");

        ArrayList<Task3Reducer> T3Reducers = new ArrayList<>(); //arraylist

        for (String Key:HMap2.keySet())
        {
            ArrayList<String[]> Value = HMap2.get(Key);
            Task3Reducer Reduce3 = new Task3Reducer(Key, Value);
            Reduce3.start();
            T3Reducers.add(Reduce3);
        }

        for (int i=0; i<=T2Reducers.size()-1; i++)
        {
            T3Reducers.get(i).join();

            writer3.println(T3Reducers.get(i).getRValue());

        }
        writer3.close();
    }

    public static ArrayList<String[]> GetDataFromCSV(ArrayList<String[]> Data, String[] ListOfAirportsCode) throws IOException
    {
        FileWriter bw = new FileWriter("ErrorLogs.txt", true);
        BufferedWriter writer = new BufferedWriter(bw);
        int x=1;
        BufferedReader br = new BufferedReader(new FileReader("AComp_Passenger_data.csv"));
        String newLine;

        while ((newLine = br.readLine()) != null) {
        String [] array = newLine.split(",");
        if (array[0].equals("")||array[1].equals("")||array[2].equals("")||array[3].equals("")||array[4].equals("")||array[5].equals(""))
        {
            writer.write("Error: Null field found.                               Discarding row:  "+x+"\r\n");
            x++;
        }
        else
        {
            int Length = array[0].length();
            array[0] = array[0].replaceAll("\\P{Print}", "");
            if ( Length != array[0].length())
            {
                writer.write("Error: Hidden character found in:\t"+array[0]+",    Correcting row: \t"+x+"\r\n");

            }

            boolean error = false;
            if (!(Character.isLetter(array[0].codePointAt(0))&&Character.isUpperCase(array[0].codePointAt(0))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x+"\r\n");
            }
            else if (!(Character.isLetter(array[0].codePointAt(1)) &&Character.isUpperCase(array[0].codePointAt(1))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x+"\r\n");


            }
            else if (!(Character.isLetter(array[0].codePointAt(2))&&Character.isUpperCase(array[0].codePointAt(2))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x+"\r\n");


            }
            else if (!(Character.isDigit(array[0].codePointAt(3))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x+"\r\n");


            }
            else if (!(Character.isDigit(array[0].codePointAt(4))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x+"\r\n");

            }
            else if (!(Character.isDigit(array[0].codePointAt(5))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[0]+",   Discarding row: \t"+x+"\r\n");

            }
            else if (!(Character.isDigit(array[0].codePointAt(6))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x+"\r\n");

            }
            else if (!(Character.isLetter(array[0].codePointAt(7))&&Character.isUpperCase(array[0].codePointAt(7))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x+"\r\n");

            }
            else if (!(Character.isLetter(array[0].codePointAt(8))&&Character.isUpperCase(array[0].codePointAt(8))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x+"\r\n");

            }
            else if (!(Character.isDigit(array[0].codePointAt(9))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[0]+",    Discarding row: \t"+x+"\r\n");

            }
            else if (!(Character.isLetter(array[2].codePointAt(0))&&Character.isUpperCase(array[2].codePointAt(0))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[2]+",           Discarding row:\t"+x+"\r\n");

            }
            else if (!(Character.isLetter(array[2].codePointAt(1))&&Character.isUpperCase(array[2].codePointAt(1))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[2]+",           Discarding row: \t"+x+"\r\n");
            }
            else if (!(Character.isLetter(array[2].codePointAt(2))&&Character.isUpperCase(array[2].codePointAt(2))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[2]+",           Discarding row: \t"+x+"\r\n");
            }
            else if (array[0].length() != 10 )
            {
                error = true;
                writer.write("Error: Illegal Passenger ID length: \t"+array[0]+" Discarding row: \t"+x+"\r\n");
            }
            else if (array[1].length() != 8 )
            {
                error = true;
                writer.write("Error: Illegal Flight ID length: \t"+array[1]+"      Discarding row: \t"+x+"\r\n");
            }
            else if (!(Character.isLetter(array[1].codePointAt(0))&&Character.isUpperCase(array[1].codePointAt(0))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x+"\r\n");
            }
            else if (!(Character.isLetter(array[1].codePointAt(1)) &&Character.isUpperCase(array[1].codePointAt(1))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x+"\r\n");            }
            else if (!(Character.isLetter(array[1].codePointAt(2))&&Character.isUpperCase(array[1].codePointAt(2))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x+"\r\n");            }
            else if (!(Character.isDigit(array[1].codePointAt(3))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x+"\r\n");
            }
            else if (!(Character.isDigit(array[1].codePointAt(4))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x+"\r\n");
            }
            else if (!(Character.isDigit(array[1].codePointAt(5))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x+"\r\n");
            }
            else if (!(Character.isDigit(array[1].codePointAt(6))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x+"\r\n");
            }
            else if (!(Character.isLetter(array[1].codePointAt(7))&&Character.isUpperCase(array[1].codePointAt(7))))
            {
                error = true;
                writer.write("Error: Disallowed character found: \t"+array[1]+",      Discarding row: \t"+x+"\r\n");
            }

                ArrayList<String> ListOfAirports = new ArrayList<>(Arrays.asList(ListOfAirportsCode));

                for (int i=0; i<=ListOfAirportsCode.length; i++)
                {
                    if ((!(ListOfAirports.contains(array[2])))&&error==false)
                    {
                        error=true;
                        writer.write("Error: Illegal airport detected: \t"+array[2]+"            Discarding row: \t"+x+"\r\n");
                        break;
                    }
                }

                for (int i=0; i<=ListOfAirportsCode.length; i++)
                {
                    if ((!(ListOfAirports.contains(array[3])))&&error==false)
                    {
                        error=true;
                        writer.write("Error: Illegal airport detected: \t"+array[3]+"            Discarding row: \t"+x+"\r\n");
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

        return Data;
    }

    public static HashMap<String,  ArrayList<String[]>> Task1Shuffler(HashMap<String,  ArrayList<String[]>> HMap, ArrayList<Task1JobObject> Mapper ) throws IOException
    {
        Task1JobObject Single = null;

        while (Mapper.isEmpty()!=true)
        {
            Single = Mapper.get(Mapper.size()-1);
            Mapper.remove(Mapper.size()-1);

            if( HMap.size()==0)
            {
                ArrayList<String[]> Payload = new ArrayList<>();
                Payload.add(Single.Payload);
                HMap.put(Single.KeyDeparture, Payload);
            }
            else
            {
                for (int j =0; j <= HMap.size(); j++)
                {
                    ArrayList<String[]> Payload = new ArrayList<>();
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

    public static HashMap<String,  ArrayList<String[]>> Task2Shuffler(HashMap<String,  ArrayList<String[]>> HMap, ArrayList<Task2JobObject> Mapper ) throws IOException
    {
        Task2JobObject Single = null;

        while (Mapper.isEmpty()!=true)
        {
            Single = Mapper.get(Mapper.size()-1);
            Mapper.remove(Mapper.size()-1);

            if( HMap.size()==0)
            {
                ArrayList<String[]> Payload = new ArrayList<>();
                Payload.add(Single.Payload);
                HMap.put(Single.FlightIDDeparture, Payload);
            }
            else
            {
                for (int j =0; j <= HMap.size(); j++)
                {
                    ArrayList<String[]> Payload = new ArrayList<>();
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

    public static String[][] getAirports() throws IOException
    {
        FileWriter bw = new FileWriter("ErrorLogs.txt", false);
        BufferedWriter writer = new BufferedWriter(bw);
        BufferedReader br = new BufferedReader(new FileReader("Top30_airports_LatLong.csv"));

        String newLine;
        String[][] output = new String[2][30];
        int i=0;
        while ((newLine = br.readLine()) != null) {

            String[] array = newLine.split(",");
            if (!array[0].equals(""))
            {
                if (!array[1].equals(""))
                {
                    output[1][i] = array[1];
                    output[0][i] = array[0];
                    i++;
                }
                else
                {
                    writer.write("Error: Null field found in airport file.               Discarding row:  "+i+"\r\n");

                }
            }
            else
            {
                writer.write("Error: Null field found in airport file.               Discarding row:  "+i+"\r\n");
            }
        }
        br.close();
        writer.close();
        return output;
    }


}