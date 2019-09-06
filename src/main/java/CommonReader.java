import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CommonReader {
    protected ArrayList<String> tableNamelist = new ArrayList<String>();
    protected ArrayList<List> tableColumnslist =new ArrayList<List>();
    protected ArrayList<List> tableDatalist = new ArrayList<List>();
    protected String filepath;
    protected InputStreamReader reader;

    CommonReader(){

    }

    CommonReader(String filepath){
        this.filepath = filepath;
    }

    public ArrayList<String> getTablenamelist() {
        return tableNamelist;
    }

    public ArrayList<List> getTableColumnslist() {
        return tableColumnslist;
    }

    public ArrayList<List> readInFile() {
        return tableDatalist;
    }

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
