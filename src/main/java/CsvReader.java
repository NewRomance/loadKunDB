import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CsvReader extends CommonReader{

    public CsvReader(String filepath, String tablename){
        super(filepath);
        this.tableNamelist.add(tablename);
    }

    public ArrayList<List> readInFile(){
        startReader();
        try{
            CSVParser parser = CSVFormat.RFC4180.parse(reader);
            List<CSVRecord> Records = parser.getRecords();
            // get tableColumns
            if(Records.size()>0) {
                CSVRecord header = Records.get(0);
                if(header != null){
                    int columnsSize = header.size();
                    ArrayList<String> tableColumns = new ArrayList<String>();
                    for(int i=0;i<columnsSize;i++){
                        tableColumns.add(header.get(i));
                    }
                    this.tableColumnslist.add(tableColumns);
                }
                Records.remove(0);
                this.tableDatalist.add(Records);
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        closeReader();
        return tableDatalist;
    }

}
