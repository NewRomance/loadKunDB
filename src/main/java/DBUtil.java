import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.time.DateUtils;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DBUtil {
    static IConnectionPool connectionPool;

    public DBUtil(String dbfilepath){
        this.connectionPool = ConnectionPoolImpl.getInstance(dbfilepath);
    }

    public static boolean insertBatch(String sql, List records, int startIndex, int endIndex, Connection conn, String tableName){
        boolean flag = true;
        PreparedStatement preStmt = null;
        try  {
            DatabaseMetaData dbmd = conn.getMetaData();
          //  System.out.println(tableName);
            ResultSet cols = dbmd.getColumns(null,null, tableName,null);
            ArrayList<String> columnTypes = new ArrayList<String>();
            preStmt=conn.prepareStatement(sql);
            conn.setAutoCommit(false);
            int[] affectedNums;
            Object object = records.get(startIndex);
            if(object instanceof CSVRecord){
                CSVRecord record = (CSVRecord) records.get(startIndex);
                for(int j = 1; cols != null && j <= record.size(); j++){
                    cols.next();
                    columnTypes.add(cols.getString("TYPE_NAME"));
                }

                for(int i = startIndex; i < endIndex; i++){
                    record = (CSVRecord) records.get(i);
                    for(int j = 0 ; j < record.size(); j++){
                        String colType = columnTypes.get(j);
                        String colValue = record.get(j);
                        if(colType.equals("DATE")|| colType.equals("DATETIME"))
                        {
                            if(colValue.equals(""))
                                colValue = null;
                            else
                                colValue = formatDateOrDateTime(colType, colValue);
                        }
                        preStmt.setObject(j+1,colValue);
                    }
                    preStmt.addBatch();
                }
                affectedNums=preStmt.executeBatch();
                //check affectedNums
                for(int i = 0; i<affectedNums.length; i++){
                    if(affectedNums[i] <= 0){
                        flag = false;
                        break;
                    }
                }
                conn.commit();
            }
            if(object instanceof String[]){
                String[] record = (String[]) records.get(startIndex);
                for(int j = 1; cols != null && j <= record.length; j++){
                    cols.next();
                    columnTypes.add(cols.getString("TYPE_NAME"));
                }

                for(int i = startIndex; i < endIndex; i++){
                    record = (String[]) records.get(i);
                    for(int j = 0 ; j < record.length; j++){
                        String colType = columnTypes.get(j);
                        String colValue = record[j];
                        if(colType.equals("DATE")|| colType.equals("DATETIME"))
                        {
                            if(colValue.equals(""))
                                colValue = null;
                            else
                                colValue = formatDateOrDateTime(colType, colValue);
                        }
                        preStmt.setObject(j+1,colValue);
                    }
                    preStmt.addBatch();
                }
                affectedNums=preStmt.executeBatch();
                //check affectedNums
                for(int i = 0; i<affectedNums.length; i++){
                    if(affectedNums[i] <= 0){
                        flag = false;
                        break;
                    }
                }
                conn.commit();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (preStmt != null) try {
                preStmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return flag;
    }

    public static String formatDateOrDateTime(String dateType, String dateStr){
        String[] possibleDateFormats =
                {
                        "yyyy-MM-dd",
                        "yyyy-MM-dd HH:mm:ss",
                        "yyyyMMdd",
                        "yyyy/MM/dd",
                        "yyyy/MM/dd HH:mm:ss",
                        "yyyy MM dd"
                };
        SimpleDateFormat newDf = new SimpleDateFormat("yyyy-MM-dd");//设置日期格式
        if(dateType.equals("DATETIME")) {
            newDf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        try{
            Date date = DateUtils.parseDate(dateStr, possibleDateFormats);
            dateStr = newDf.format(date);
        }catch (java.text.ParseException e){
            e.printStackTrace();
        }
        return dateStr;
    }

    public static void createAndInsertSql(final String tableName, List<String> tableColumns, final List records, int threadNum){
        //prepared statement sql
        String insertSql = "insert into "+tableName+"(";
        int columnsSize = tableColumns.size();
        for(int i = 0; i< columnsSize;i++){
            insertSql+=tableColumns.get(i);
            if( i != columnsSize-1){
                insertSql+=", ";
            }
        }
        insertSql+=") values(";
        for(int i = 0; i< columnsSize;i++){
            insertSql+="?";
            if( i != columnsSize-1){
                insertSql+=", ";
            }
        }
        insertSql+=")";

        final ExecutorService executorService = Executors.newCachedThreadPool();
        int startIndex = 0;
        int endIndex = 0;
        int recordsSize = records.size();
        int interval = recordsSize/threadNum;
        System.out.println("file size: "+ recordsSize);
        if(interval==0) interval = recordsSize;
        final String preSql = insertSql;
        while(recordsSize>0 && endIndex<recordsSize){
            startIndex = endIndex;
            endIndex = startIndex + interval;
            if(endIndex>recordsSize){
                endIndex = recordsSize;
            }
            final int start = startIndex;
            final int end = endIndex;
          //  System.out.println(start+" "+end+" "+interval);
            executorService.execute(new Runnable() {
                public void run() {
                    Connection conn = connectionPool.getConnection();
                  // System.out.println(conn+" "+start+" "+end);
                    if(!insertBatch(preSql,records,start,end,conn,tableName)){
                        //log error;
                        System.out.println("Fail to insert batch records into "+tableName+" when records' ranges between("+start+","+end+")");
                        //return;
                    }else{
                        System.out.println("Success to insert batch records into "+tableName+" when records' ranges between("+start+","+end+")");
                    }
                    freeConnectionPool();
                }
            });
        }
        executorService.shutdown();
    }

    public static void freeConnectionPool(){
        connectionPool.freeLocalConnection();
    }
}
