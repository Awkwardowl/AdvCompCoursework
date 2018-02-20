import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;

public class AdvComp {

    public static void main(String[] args) throws IOException
    {

        ArrayList<Task1ReduceObject> Mapper = new ArrayList<Task1ReduceObject>();
        ArrayList<String[]> DataL = new ArrayList<String[]>();
        String[][] Data = new String[388][8];
        String[] Row = new String[8];

        DataL = GetDataFromCSV(DataL);

        for (int i=0; i <= DataL.size(); i++)
        {
            Row[0] = Data[i][0];
            Row[1] = Data[i][1];
            Row[2] = Data[i][2];
            Row[3] = Data[i][3];
            Row[4] = Data[i][4];
            Row[5] = Data[i][5];
            Row[6] = Data[i][6];
            Row[7] = Data[i][7];

            Mapper.add(new Task1ReduceObject(Row));
        }
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

//                    Data.add(Row);
//                    Data[x][y] = scanner.next();
                }
                Data.add(Row);
            }
            scanner.close();
            x++;
        }
        br.close();
        return Data;
    }
}