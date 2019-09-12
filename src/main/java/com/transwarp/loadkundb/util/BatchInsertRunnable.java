package com.transwarp.loadkundb.util;

import javax.print.DocFlavor;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class BatchInsertRunnable implements Runnable {
    public DBUtil dbUtil;
    public String tableName;
    public String preSql;
    public List batchRecords;
    public int currentStart;
    public int currentEnd;
    public BatchInsertRunnable(final DBUtil dbUtil, final String tableName, final String preSql, final List batchRecords, final int currentStart, final int currentEnd){
        this.dbUtil = dbUtil;
        this.tableName =tableName;
        this.preSql = preSql;
        this.batchRecords = batchRecords;
        this.currentStart = currentStart;
        this.currentEnd = currentEnd;
    }
    public void run() {
        Connection conn = dbUtil.getConnection();
        ArrayList<String> columnTypes = dbUtil.getColumnTypes(tableName,conn);
        if(!dbUtil.insertBatch(preSql,batchRecords,conn,columnTypes)){
            //log error;
            System.out.println("Fail to insert batch records into "+tableName+" when records' ranges between["+currentStart+","+currentEnd+")");
            //return;
        }else{
            System.out.println("Success to insert batch records into "+tableName+" when records' ranges between["+currentStart+","+currentEnd+")");
        }
        dbUtil.freeConnectionPool();
    }
}
