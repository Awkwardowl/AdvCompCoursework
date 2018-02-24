import java.io.IOException;
import java.util.ArrayList;

public class Task1Mapper extends Thread {
    @Override
    public void run() {
    }

    public static ArrayList<Task1JobObject> MapperTask1(ArrayList<String[]> Data, ArrayList<Task1JobObject> Mapper ) throws IOException
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
}
