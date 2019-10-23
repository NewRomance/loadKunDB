import java.io.BufferedReader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class textReader extends CommonReader {
    String separator;
    Boolean hasHeaders;
    public textReader(String filePath, String tablename, int batchSize, int threadsNum, String separator, Boolean hasHeaders, String encoding){
        super(filePath,encoding);
        this.separator = separator;
        this.batchSize = batchSize;
        this.threadsNum = threadsNum;
        this.tableNamelist.add(tablename);
        this.hasHeaders = hasHeaders;
    }

    public void readAndHandle(final DBUtil dbUtil){
        startReader();
        try{
            BufferedReader bufferedReader = new BufferedReader(this.reader);
            String lineTxt;
            int count = 0;
            int sum = 0;
            List<String[]> records =  new ArrayList<String[]>();
            List<String> tableColumns= new ArrayList<String>();
            List<String> columnTypes;

            String preStmtStr="";

            /** get columns names and columns types. */
            Connection conn = DBUtil.getConnection();
            columnTypes=dbUtil.getColumnNamesAndTypes(tableNamelist.get(0),tableColumns,conn);
            preStmtStr=dbUtil.createPrepareStatement(tableNamelist.get(0),tableColumns);
            DBUtil.freeConnectionPool();

            final ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
            while ((lineTxt = bufferedReader.readLine()) != null) {
                if(this.hasHeaders){
                    hasHeaders = false;
                    continue;
                }
                records.add(lineTxt.split(separator));
                count++;
                sum++;
                if(count == batchSize){
                    executorService.execute(new BatchInsertRunnable(dbUtil,tableNamelist.get(0),columnTypes,preStmtStr,records,sum-batchSize,sum));
                    count = 0;
                    records = new ArrayList<String[]>();
                }
            }
            /** handle the rest records.*/
            if(records.size() > 0){
                int currentStart = sum - records.size();
                if(currentStart < 0) currentStart =0;
                executorService.execute(new BatchInsertRunnable(dbUtil,tableNamelist.get(0),columnTypes,preStmtStr,records,currentStart,sum));
            }
            executorService.shutdown();

            /**自旋*/
            while (true){
                if (executorService.isTerminated()){
                    System.out.println("INFO: all threads are terminated.");
                    DBUtil.destroyConnectionPool();
                    break;
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        closeReader();
    }

}
