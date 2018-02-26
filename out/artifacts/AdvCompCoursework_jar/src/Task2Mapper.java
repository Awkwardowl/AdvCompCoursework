import java.util.ArrayList;
import java.util.List;

/**
 * Mapper for Task 2 and 3. Can be created as a new thread.
 */
public class Task2Mapper extends Thread { //extends thread to allow threads to be made
    List<String[]> Data;
    ArrayList<Task2JobObject> Mapper;

    /**
     * getter for mapper.
     * @return
     */
    public ArrayList<Task2JobObject> getMapper() {
        return Mapper;
    } //allows getting of the mapper.

    /**
     * construtor for mapper task1.
     * @param DataI
     */
    public Task2Mapper(List<String[]> DataI) {
        Data = DataI;
        Mapper = new ArrayList<>();
    }

    @Override
    public void run() {
        for(String[] array:Data) //for every string in data.
        {
            Mapper.add(new Task2JobObject(array)); //create new object assign to mapper.
        }
    }
}
