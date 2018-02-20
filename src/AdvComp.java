import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

public class AdvComp {


    public static void main(String[] args) throws IOException
    {
        String[][] Data = new String[400][8];
        String[][] FlightsCount = new String[50][2];

        BufferedReader br = new BufferedReader(new FileReader("AComp_Passenger_data_no_error.csv"));
        String newLine;
        int x=0;

        while ((newLine = br.readLine()) != null)
        {

            Scanner scanner = new Scanner(newLine);
            scanner.useDelimiter(",");

            while(scanner.hasNext())
            {
                for (int y=0; y < 8; y++)
                {
                    Data[x][y] = scanner.next();
                }
            }
            scanner.close();
            x++;
        }
     



    }
}