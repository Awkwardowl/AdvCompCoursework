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
}
