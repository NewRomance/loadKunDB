import javax.print.DocFlavor;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class BatchInsertRunnable implements Runnable {
    DBUtil dbUtil;
    String tableName;
    List<String> columnTypes;
    String preSql;
    List batchRecords;
    int currentStart;
    int currentEnd;
    BatchInsertRunnable(final DBUtil dbUtil, final String tableName, final List<String> columnTypes, final String preSql, final List batchRecords, final int currentStart, final int currentEnd){
        this.dbUtil = dbUtil;
        this.tableName =tableName;
        this.preSql = preSql;
        this.batchRecords = batchRecords;
        this.currentStart = currentStart;
        this.currentEnd = currentEnd;
        this.columnTypes = columnTypes;
    }
    public void run() {
        Connection conn = dbUtil.getConnection();
        if(!dbUtil.insertBatch(preSql,batchRecords,conn,columnTypes)){
            //log error;
            System.out.println("ERROR: fail to insert batch records into "+tableName+" when records' ranges between["+currentStart+","+currentEnd+")");
        }else{
            System.out.println("INFO: success to insert batch records into "+tableName+" when records' ranges between["+currentStart+","+currentEnd+")");
        }
        dbUtil.freeConnectionPool();
    }
}
