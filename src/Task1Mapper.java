import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Task1Mapper extends Thread {
    List<String[]> Data;
    ArrayList<Task1JobObject> Mapper;

    public ArrayList<Task1JobObject> getMapper() {
        return Mapper;
    }

    public Task1Mapper(List<String[]> DataI) {
        Data = DataI;
        Mapper = new ArrayList<>();
    }

    @Override
    public void run() {
        for(String[] array:Data)
        {
            Mapper.add(new Task1JobObject(array));
        }
    }

}
