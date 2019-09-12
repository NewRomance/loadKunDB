package com.transwarp.loadkundb.read;

import com.transwarp.loadkundb.common.CommonReader;
import com.transwarp.loadkundb.util.BatchInsertRunnable;
import com.transwarp.loadkundb.util.DBUtil;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TextReader extends CommonReader {
    String separator;

    public TextReader(String filePath, String tablename, int batchSize, String separator){
        super(filePath);
        this.separator = separator;
        this.batchSize = batchSize;
        this.tableNamelist.add(tablename);
    }

    public void readAndHandle(final DBUtil dbUtil){
        try{
            startReader();
            BufferedReader bufferedReader = new BufferedReader(this.reader);
            String lineTxt;
            int count = 0;
            int sum = 0;
            List<String[]> records =  new ArrayList<String[]>();
            List<String> tableColumns;
            String preStmtStr="";
            boolean isHeader = true;
            ExecutorService executorService = Executors.newCachedThreadPool();
            while ((lineTxt = bufferedReader.readLine()) != null) {
                if(isHeader){
                    tableColumns = Arrays.asList(lineTxt.split(separator));
                    preStmtStr = dbUtil.createPrepareStatement(tableNamelist.get(0),tableColumns);
                    isHeader = false;
                    continue;
                }
                records.add(lineTxt.split(separator));
                count++;
                sum++;
                if(count == batchSize){
                    executorService.execute(new BatchInsertRunnable(dbUtil,tableNamelist.get(0),preStmtStr,records,sum-batchSize,sum));
                    count = 0;
                    records = new ArrayList<String[]>();
                }
            }
            // handle the rest records.
            if(records.size() > 0){
                int currentStart = sum - count;
                if(currentStart < 0) currentStart =0;
                executorService.execute(new BatchInsertRunnable(dbUtil,tableNamelist.get(0),preStmtStr,records,currentStart,sum));
            }
            executorService.shutdown();
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            closeReader();
        }
    }

}
