import java.io.BufferedReader;
import java.lang.reflect.Array;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TxtReader extends CommonReader {
    String separator;
    public TxtReader(String filePath,String tablename,String separator){
        super(filePath);
        this.separator = separator;
        this.tableNamelist.add(tablename);
    }
    public ArrayList<List> readInFile(){
        startReader();
        try{
            BufferedReader bufferedReader = new BufferedReader(this.reader);
            String lineTxt;
            List<String[]> records =  new ArrayList<String[]>();
            while ((lineTxt = bufferedReader.readLine()) != null) {
                records.add(lineTxt.split(separator));
            }
            List<String> tableColumns = Arrays.asList(records.get(0));
            tableColumnslist.add(tableColumns);
            records.remove(0);
            tableDatalist.add(records);
        }catch (Exception e){
            e.printStackTrace();
        }
        closeReader();
        return tableDatalist;
    }

}
