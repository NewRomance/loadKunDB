import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class CommonReader {
    protected ArrayList<String> tableNamelist = new ArrayList<String>();
    protected String filepath;
    protected InputStreamReader reader;
    protected int batchSize;
    protected int threadsNum;
    protected String encoding;

    CommonReader(){

    }

    CommonReader(String filepath,String encoding){
        this.filepath = filepath;
        this.encoding = encoding;
    }

    public void readAndHandle(final DBUtil dbUtil){}

    public void startReader(){
        try {
            System.out.println("INFO: start the reader.");
            reader = new InputStreamReader(new FileInputStream(filepath),encoding);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeReader(){
        try{
            if(reader != null) {
                reader.close();
                System.out.println("INFO: close the reader.");
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
