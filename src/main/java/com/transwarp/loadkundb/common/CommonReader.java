package com.transwarp.loadkundb.common;
import com.transwarp.loadkundb.util.DBUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class CommonReader  implements ShardedTableInfo {
    public String filepath;
    public InputStreamReader reader;
    public int batchSize;

    public CommonReader(){

    }

    public CommonReader(String filepath){
        this.filepath = filepath;
    }

    public void readAndHandle(final DBUtil dbUtil){}

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
