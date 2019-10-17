import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class CommonReader {
    protected ArrayList<String> tableNamelist = new ArrayList<String>();
    protected String filepath;
    protected InputStreamReader reader;
    protected int batchSize;

    CommonReader(){

    }

    CommonReader(String filepath){
        this.filepath = filepath;
    }

    public void readAndHandle(final DBUtil dbUtil){}

    public void startReader(){
        try {
            reader = new InputStreamReader(new FileInputStream(filepath), "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeReader(){
        try{
            if(reader != null)
                reader.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
