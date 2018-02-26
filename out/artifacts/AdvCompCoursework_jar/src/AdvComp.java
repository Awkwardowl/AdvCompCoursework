import java.io.*;
import java.util.*;
import java.util.Arrays;

/**
 * Main method for MapReduce prototype.
 */
public class AdvComp {

    public static void main(String[] args) throws IOException, InterruptedException {

        ArrayList<Task1JobObject> Mapper1 = new ArrayList<>(); //Two blank mapper arrays, these will hold the data used as constructor to the shuffler.
        ArrayList<Task2JobObject> Mapper2 = new ArrayList<>();

        ArrayList<String[]> Data = new ArrayList<>(); //Used to hold the inputted data from .csv

        HashMap<String,  ArrayList<String[]>>HMap = new HashMap<>(); //Hashmap used to store the Key-List<Values> for task 1
        HashMap<String,  ArrayList<String[]>>HMap2 = new HashMap<>(); //Hashmap used to store the Key-List<Values> for task 2 and 3

        ArrayList<String> AirPortsUsed = new ArrayList<>(); //Stores the list of airports used in arraylist format

        String ListOfAirportsArray [][] = getAirports(); //gets the airport names and codes from file and error checks the inputs.
        String ListOfAirportsCode[] = ListOfAirportsArray[1]; //1D array of airport codes.
        String ListOfAirportsName[] = ListOfAirportsArray[0]; //1D array of airport names.

        Data = GetDataFromCSV(Data,ListOfAirportsCode); //gets the data from the data file, error checks it and writes to errorlogs.txt when errors are found

        ArrayList<String[]> Data2 = new ArrayList<>();

        Data2.addAll(Data);//Copy of data used for Task2 and 3. Copied as I pop from the data file. Easier than recalling get data from csv.

        ArrayList<Task1Mapper> T1Mapper = new ArrayList<>(); //Array of Threads of T1 mapper

        for (int i=0; i<Data.size()-1; i++)
        {
            List<String[]> row = Data.subList(i, i+1); //gets a single row of data
            Task1Mapper mapper1 = new Task1Mapper(row); //Creates new threads
            mapper1.start(); //starts new thread
            T1Mapper.add(mapper1); //adds the thread to the array of threads.
        }

        for (int i=0; i<=T1Mapper.size()-1; i++)
        {
            T1Mapper.get(i).join(); //Joins the ended threads
            Mapper1.addAll( T1Mapper.get(i).getMapper());//Gets the data from the completed threads
        }

        HMap = Task1Shuffler(HMap, Mapper1); //runs the shuffler task returns key-list<values>

        ArrayList<String> ListOfAirports = new ArrayList<>(Arrays.asList(ListOfAirportsCode));
        ArrayList<String> ListOfAirportsNames = new ArrayList<>(Arrays.asList(ListOfAirportsName)); //both this and above change from array to more useable data format

        PrintWriter writer = new PrintWriter("ObjectiveOne.txt", "UTF-8"); //creates file to write too.

        ArrayList<Task1Reducer> T1Reducers = new ArrayList<>(); //creates an array of reducer threads.

        for (String Key:HMap.keySet()) //Creates a reducer thread for every key in HMap.
        {
            ArrayList<String[]> Value = HMap.get(Key); //passes single row to each thread
            Task1Reducer Reduce = new Task1Reducer(Key, Value,ListOfAirportsArray); //creates reducer threads
            Reduce.start(); //starts the threads
            T1Reducers.add(Reduce); //adds them to array of threads
        }

        for (int i=0; i<=T1Reducers.size()-1; i++) //iterates over every thread in the list
        {
            T1Reducers.get(i).join(); //joins the ended threads
            AirPortsUsed.add(T1Reducers.get(i).getKey()); //gets the airports used.
            writer.println(T1Reducers.get(i).getRValue()); //writes the airport to file.
        }

        String NotUsed = "";

        for(int i=0;i<=ListOfAirports.size()-1;i++){                         //This nested loop compares the two lists noting any that are not in airports used.
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
        writer.println("These airport did not get flights: "+NotUsed); //prints the airports that weren't used.
        writer.println();
        writer.close();

        ArrayList<Task2Mapper> T2Mapper = new ArrayList<>(); //creates and array of mapper threads.

        for (int i=0; i<Data.size()-1; i++) //for everyrow in the datafile
        {
            List<String[]> row = Data.subList(i, i+1); //gets a row of the data file.
            Task2Mapper mapper2 = new Task2Mapper(row); //creates new mapper thread passing the row
            mapper2.start(); //starts thread.
            T2Mapper.add(mapper2); //adds the thread to the array of threads.
        }

        for (int i=0; i<=T2Mapper.size()-1; i++) //for every thread previously created.
        {
            T2Mapper.get(i).join(); //when the threads finish.
            Mapper2.addAll( T2Mapper.get(i).getMapper()); //adds the returned key-values pairs to the main list.
        }

        HMap2 = Task2Shuffler(HMap2, Mapper2); //runs the shuffler which

        ArrayList<Task2Reducer> T2Reducers = new ArrayList<>(); //array of t2 reducer threads.

        PrintWriter writer2 = new PrintWriter("ObjectiveTwo.txt", "UTF-8"); //creates the objectivetwo.txt file ready for writing.

        for (String Key:HMap2.keySet()) //for every key in hmap2
        {
            ArrayList<String[]> Value = HMap2.get(Key); //get a single key and associated values.
            Task2Reducer Reduce2 = new Task2Reducer(Key, Value,ListOfAirportsArray); //creates new thread passing it a single row.
            Reduce2.start(); //starts the thread.
            T2Reducers.add(Reduce2); //adds thread to list of threads.
        }

        for (int i=0; i<=T2Reducers.size()-1; i++)
        {
            T2Reducers.get(i).join();//when thread is done joins thread
            writer2.println(T2Reducers.get(i).getRValue());//writes the output to file.
            ArrayList<String> passengers=T2Reducers.get(i).passengers;//gets the passenger from each thread. adds to list of passengers.
            for (int j=0; j<=passengers.size()-1; j++) //for all of the list of passengers.
            {
                writer2.println("\t"+passengers.get(j)); //writes the passenger
            }
            writer2.println("");//formatting
        }

        writer2.close(); //closes the writer.

        PrintWriter writer3 = new PrintWriter("ObjectiveThree.txt", "UTF-8"); //creates OutputThree.txt for writing.

        ArrayList<Task3Reducer> T3Reducers = new ArrayList<>(); //arraylist

        for (String Key:HMap2.keySet()) //for every key in HMap2
        {
            ArrayList<String[]> Value = HMap2.get(Key); //get a row of HMap
            Task3Reducer Reduce3 = new Task3Reducer(Key, Value); //passes a single key and values to reducer thread.
            Reduce3.start(); //starts the thread.
            T3Reducers.add(Reduce3); //adds the thread to the array of threads.
        }

        for (int i=0; i<=T2Reducers.size()-1; i++) //for every thread.
        {
            T3Reducers.get(i).join(); //join thread when completed.
            writer3.println(T3Reducers.get(i).getRValue()); //writes the output to file.
        }
        writer3.close(); //closes the writer file.
    }

    /**This method gets all the data from the .csv. Checking it for error as well as validating the information that can be validated.
     *
     * @param Data
     * @param ListOfAirportsCode Used to validate the airports within the .csv.
     * @return
     * @throws IOException
     */
    public static ArrayList<String[]> GetDataFromCSV(ArrayList<String[]> Data, String[] ListOfAirportsCode) throws IOException
    {
        FileWriter bw = new FileWriter("ErrorLogs.txt", true); //opens the ErrorLog file in amend mode.
        BufferedWriter writer = new BufferedWriter(bw); //converts file writer to bufferedwriter.
        int x=1;
        BufferedReader br = new BufferedReader(new FileReader("AComp_Passenger_data.csv")); //gets the data set.
        String newLine;

        while ((newLine = br.readLine()) != null) { //while row is not null
            String [] array = newLine.split(","); //split whenever a comma is seen.
            if (array[0].equals("")||array[1].equals("")||array[2].equals("")||array[3].equals("")||array[4].equals("")||array[5].equals("")) //checks any of the input is not null.
            {
                writer.write("Error: Null field found in Data file:                  Discarding row:  "+x+"\r\n"); //writes to error log.
                x++;
            }
            else
            {
                int Length = array[0].length();
                array[0] = array[0].replaceAll("\\P{Print}", "");
                if ( Length != array[0].length())
                {
                    writer.write("Error: Hidden character found in:\t"+array[0]+"     Correcting row: \t"+x+"\r\n");
                }

                boolean error = false;
                if (!(Character.isLetter(array[0].codePointAt(0))&&Character.isUpperCase(array[0].codePointAt(0)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[0]+"     Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isLetter(array[0].codePointAt(1)) &&Character.isUpperCase(array[0].codePointAt(1)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[0]+"     Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isLetter(array[0].codePointAt(2))&&Character.isUpperCase(array[0].codePointAt(2)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[0]+"     Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isDigit(array[0].codePointAt(3)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[0]+"     Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isDigit(array[0].codePointAt(4)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[0]+"     Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isDigit(array[0].codePointAt(5)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[0]+"    Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isDigit(array[0].codePointAt(6)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[0]+"     Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isLetter(array[0].codePointAt(7))&&Character.isUpperCase(array[0].codePointAt(7)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[0]+"     Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isLetter(array[0].codePointAt(8))&&Character.isUpperCase(array[0].codePointAt(8)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[0]+"     Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isDigit(array[0].codePointAt(9)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[0]+"     Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isLetter(array[2].codePointAt(0))&&Character.isUpperCase(array[2].codePointAt(0)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[2]+"            Discarding row:\t"+x+"\r\n");
                }
                else if (!(Character.isLetter(array[2].codePointAt(1))&&Character.isUpperCase(array[2].codePointAt(1)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[2]+"            Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isLetter(array[2].codePointAt(2))&&Character.isUpperCase(array[2].codePointAt(2)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[2]+"            Discarding row: \t"+x+"\r\n");
                }
                else if (array[0].length() != 10 ) //Checks for correct statement length
                {
                    error = true;
                    writer.write("Error: Illegal Passenger ID length: \t"+array[0]+" Discarding row: \t"+x+"\r\n");
                }
                else if (array[1].length() != 8 ) //Checks for correct statement length
                {
                    error = true;
                    writer.write("Error: Illegal Flight ID length: \t"+array[1]+"      Discarding row: \t"+x+"\r\n");
                }
                else if (array[4].length() != 10 ) //Checks for correct statement length
                {
                    error = true;
                    writer.write("Error: Illegal Epochtime length: \t"+array[1]+"      Discarding row: \t"+x+"\r\n");
                }
                else if (array[5].length() > 4 ) //Checks for correct statement length
                {
                    error = true;
                    writer.write("Error: Illegal flight time      : \t"+array[5]+"      Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isLetter(array[1].codePointAt(0))&&Character.isUpperCase(array[1].codePointAt(0)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[1]+"       Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isLetter(array[1].codePointAt(1)) &&Character.isUpperCase(array[1].codePointAt(1)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[1]+"       Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isLetter(array[1].codePointAt(2))&&Character.isUpperCase(array[1].codePointAt(2))))//Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[1]+"       Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isDigit(array[1].codePointAt(3)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[1]+"       Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isDigit(array[1].codePointAt(4)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[1]+"       Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isDigit(array[1].codePointAt(5)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[1]+"       Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isDigit(array[1].codePointAt(6)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[1]+"       Discarding row: \t"+x+"\r\n");
                }
                else if (!(Character.isLetter(array[1].codePointAt(7))&&Character.isUpperCase(array[1].codePointAt(7)))) //Checks for correct character syntax
                {
                    error = true;
                    writer.write("Error: Disallowed character found: \t"+array[1]+"       Discarding row: \t"+x+"\r\n");
                }

                ArrayList<String> ListOfAirports = new ArrayList<>(Arrays.asList(ListOfAirportsCode));

                for (int i=0; i<=ListOfAirportsCode.length; i++)
                {
                    if ((!(ListOfAirports.contains(array[2])))&&error==false)
                    {
                        error=true;
                        writer.write("Error: Illegal airport detected: \t"+array[2]+"            Discarding row: \t"+x+"\r\n"); //check airport is a legal airport.
                        break;
                    }
                }

                for (int i=0; i<=ListOfAirportsCode.length; i++)
                {
                    if ((!(ListOfAirports.contains(array[3])))&&error==false)
                    {
                        error=true;
                        writer.write("Error: Illegal airport detected: \t"+array[3]+"            Discarding row: \t"+x+"\r\n"); //check airport is a legal airport.
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
                    Data.add(Row); //adds the data to the row if all field pass correct.
                }
                x++;
            }

        }
        br.close(); //closes buffered reader
        writer.close(); //closes writer

        return Data; //returns the array list of data.
    }

    /**The is the shuffler/sort section for task 1. It turns Key-Value pairs into Key-ListValues.
     *
     * @param HMap
     * @param Mapper List of Key-Values.
     * @return
     * @throws IOException
     */
    public static HashMap<String,  ArrayList<String[]>> Task1Shuffler(HashMap<String,  ArrayList<String[]>> HMap, ArrayList<Task1JobObject> Mapper ) throws IOException
    {
        Task1JobObject Single = null; //temp hold object

        while (Mapper.isEmpty()!=true) //whilst mapper isn't empty
        {
            Single = Mapper.get(Mapper.size()-1); //gets a single row.
            Mapper.remove(Mapper.size()-1); //pops mapper.

            if( HMap.size()==0) //if empty add irregardless of data.
            {
                ArrayList<String[]> Payload = new ArrayList<>();
                Payload.add(Single.Payload);
                HMap.put(Single.KeyDeparture, Payload);
            }
            else
            {
                for (int j =0; j <= HMap.size(); j++) //if not empty check if it contains the key, if it does get the value add new value to and read.
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
                        HMap.put(Single.KeyDeparture, Payload); //if not contained add to the hashmap with original payload.
                        break;
                    }
                }
            }
        }
        return HMap; //return the hashmap to main.
    }

    /**The is the shuffler/sort section for task 2 and 3. It turns Key-Value pairs into Key-ListValues.
     *
     * @param HMap
     * @param Mapper
     * @return
     * @throws IOException
     */
    public static HashMap<String,  ArrayList<String[]>> Task2Shuffler(HashMap<String,  ArrayList<String[]>> HMap, ArrayList<Task2JobObject> Mapper ) throws IOException
    {
        Task2JobObject Single = null; //temp hold object

        while (Mapper.isEmpty()!=true) //while mapper isn't empty
        {
            Single = Mapper.get(Mapper.size()-1); //gets a single row.
            Mapper.remove(Mapper.size()-1); //pops mapper.

            if( HMap.size()==0) //if empty add irregardless of data.
            {
                ArrayList<String[]> Payload = new ArrayList<>();
                Payload.add(Single.Payload);
                HMap.put(Single.FlightIDDeparture, Payload);
            }
            else
            {
                for (int j =0; j <= HMap.size(); j++) //if not empty check if it contains the key, if it does get the value add new value to and read.
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
                        HMap.put(Single.FlightIDDeparture, Payload); //if not contained add to the hashmap with original payload.
                        break;
                    }
                }
            }
        }
        return HMap;
    }

    /**
     * This gets the airport names and codes from the .csv. It checks for validity against the syntax in the assignment brief.
     * @return
     * @throws IOException
     */
    public static String[][] getAirports() throws IOException
    {
        FileWriter bw = new FileWriter("ErrorLogs.txt", false); //creates and opens the errorlogs.txt file
        BufferedWriter writer = new BufferedWriter(bw); // converts filewriter to buffered writer.
        BufferedReader br = new BufferedReader(new FileReader("Top30_airports_LatLong.csv")); //opens Airport list as a csv.

        String newLine;
        String[][] output = new String[2][30];
        int i=0;
        while ((newLine = br.readLine()) != null) { //Whiles file new line in not null

            String[] array = newLine.split(","); //split on the comma.
            if (!array[0].equals("")) //column 0 not null
            {
                if (!array[1].equals("")) //column 1 not null
                {
                    boolean error = false;
                    if (!(array[0].length() >=20)) //airport name not larger than 20 characters.
                    {
                    if (!(Character.isLetter(array[1].codePointAt(0))&&Character.isUpperCase(array[1].codePointAt(0)))) //airport code syntax checking
                    {
                        error = true;
                        writer.write("Error: Disallowed character found: \t"+array[1]+",           Discarding row:\t"+i+"\r\n");

                    }
                    else if (!(Character.isLetter(array[1].codePointAt(1))&&Character.isUpperCase(array[1].codePointAt(1)))) //airport code syntax checking
                    {
                        error = true;
                        writer.write("Error: Disallowed character found: \t"+array[1]+",           Discarding row: \t"+i+"\r\n");
                    }
                    else if (!(Character.isLetter(array[1].codePointAt(2))&&Character.isUpperCase(array[1].codePointAt(2)))) //airport code syntax checking
                    {
                        error = true;
                        writer.write("Error: Disallowed character found: \t"+array[1]+",           Discarding row: \t"+i+"\r\n");
                    }
                    else if (error==false) {
                        output[1][i] = array[1];
                        output[0][i] = array[0];
                        i++;
                    }

                    }
                }
                else
                {
                    writer.write("Error: Null field found in airport file:               Discarding row:  "+i+"\r\n"); //writes error to log.
                }
            }
            else
            {
                writer.write("Error: Null field found in airport file:               Discarding row:  "+i+"\r\n"); //writes error to log.
            }
        }
        br.close(); //closes buffered reader.
        writer.close(); //close writer.
        return output; //returns the output file.
    }


}