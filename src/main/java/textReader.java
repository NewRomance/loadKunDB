import java.io.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class textReader extends CommonReader {
    static String separator;
    Boolean hasHeaders;
    static String enclosedChar;
    static String failedFileDir;
    static int allowFailedFileCount;

    public textReader(String filePath, String tablename, int batchSize, int threadsNum, String separator, String enclosedChar, Boolean hasHeaders, String encoding, String failedFileDir, int FailedFileCount){
        super(filePath,encoding);
        this.separator = separator;
        this.enclosedChar = enclosedChar;
        this.batchSize = batchSize;
        this.threadsNum = threadsNum;
        this.tableNamelist.add(tablename);
        this.hasHeaders = hasHeaders;
        this.failedFileDir = failedFileDir;
        this.allowFailedFileCount = FailedFileCount;
        File file = new File(failedFileDir);
        if (!file.exists()){
            file.mkdirs();
        }else if(!file.isDirectory()){
            System.out.println("ERROR: file already exists, please rename the directory.");
            System.exit(0);
        }
        System.out.println(allowFailedFileCount);
    }

    /**
     * 将执行失败的批次的记录写到文件夹中，名称为start_end.txt
     * */
    public static synchronized void failedFileBackUp(List<String[]> recordlist, int start, int end){
        if(allowFailedFileCount > 0) {
            String filename = start + "_" + end + ".txt";
            String filepath = failedFileDir + "/" + filename;
            File file = new File(filepath);
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (java.io.IOException e) {
                    System.out.println("ERROR: fail to create new file " + filename + ".");
                    e.printStackTrace();
                }
            }
            // write record list into file.
            try {
                FileOutputStream fos = new FileOutputStream(file);
                String[] record = null;
                // use StringBuilder to avoid frequent GC.
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < recordlist.size(); i++) {
                    record = recordlist.get(i);
                    for (int j = 0; j < record.length; j++) {
                        builder.append(enclosedChar+record[j]+enclosedChar);
                        if (j != record.length - 1) {
                            builder.append(separator);
                        }
                    }
                    builder.append("\n");
                }
                fos.write(builder.toString().getBytes(encoding));
                System.out.println("WARN: write failed batch data into "+ filename);
                fos.close();
            } catch (java.io.IOException e) {
                System.out.println("ERROR: can't create fileWriter.");
                e.printStackTrace();
            }
            allowFailedFileCount-=1;
        }else{
            System.out.println("ERROR: allow failedFileCount has meet the maximum.");
            System.exit(0);
        }
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

            String preStmtStr;

            /** get columns names and columns types. */
            Connection conn = DBUtil.getConnection();
            columnTypes=dbUtil.getColumnNamesAndTypes(tableNamelist.get(0),tableColumns,conn);

            System.out.println("INFO: columnTypes size = " + columnTypes.size());
            if(columnTypes.size() == 0){
                System.out.println("ERROR: unable to find table"+ tableNamelist.get(0)+", please confirm your params");
                System.exit(0);
            }

            preStmtStr=dbUtil.createPrepareStatement(tableNamelist.get(0),tableColumns);
            DBUtil.freeConnectionPool();

            int colsSize = tableColumns.size();

            final ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);

            /**
             * timer
             * */
            long t1 = System.currentTimeMillis();

            while ((lineTxt = bufferedReader.readLine()) != null) {
                if(this.hasHeaders){
                    hasHeaders = false;
                    continue;
                }
                /**
                 *split以最后一个非空字符为准开始分割,例如“，，，‘’”,分割完后长度为4，“，，‘’，”，分割完后长度为3。
                 * */
                String[] record = lineTxt.trim().split(separator);
                /**
                 * 把最后的空值补齐
                 * */
                if(record.length < colsSize){
                    String[] completeRecord = new String[colsSize];
                    for (int i = 0; i < colsSize; i++){
                        if(i < record.length)
                            completeRecord[i] = record[i];
                        else
                            completeRecord[i] = null;
                    }
                    records.add(completeRecord);
                }else
                    records.add(record);

                count++;
                sum++;
                if(count == batchSize){
                    executorService.execute(new BatchInsertRunnable(dbUtil,tableNamelist.get(0),columnTypes,preStmtStr,records,enclosedChar,sum-batchSize,sum));
                    count = 0;
                    records = new ArrayList<String[]>();
                }
            }
            /** handle the rest records.*/
            if(records.size() > 0){
                int currentStart = sum - records.size();
                if(currentStart < 0) currentStart =0;
                executorService.execute(new BatchInsertRunnable(dbUtil,tableNamelist.get(0),columnTypes,preStmtStr,records,enclosedChar,currentStart,sum));
            }
            executorService.shutdown();

            /**自旋*/
            while (true){
                if (executorService.isTerminated()){
                    System.out.println("INFO: all threads are terminated.");
                    long timeConsumed = System.currentTimeMillis() - t1;
                    System.out.println("INFO: time consumed " + timeConsumed +"ms");
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