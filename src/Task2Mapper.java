import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Task2Mapper extends Thread {
    List<String[]> Data;
    ArrayList<Task2JobObject> Mapper;

    public ArrayList<Task2JobObject> getMapper() {
        return Mapper;
    }

    public Task2Mapper(List<String[]> DataI) {
        Data = DataI;
        Mapper = new ArrayList<>();
    }

    @Override

    public void run() {
        for(String[] array:Data)
        {
            Mapper.add(new Task2JobObject(array));
        }
    }

//    public static ArrayList<Task2JobObject> MapperTask2(ArrayList<String[]> Data, ArrayList<Task2JobObject> Mapper ) throws IOException
//    {
//        String[] Row = new String[8];
//        while (Data.isEmpty() != true)
//        {
//            Row = Data.get(Data.size()-1);
//            Data.remove(Data.size()-1);
//            Mapper.add(new Task2JobObject(Row));
//        }
//        return Mapper;
//    }

}
