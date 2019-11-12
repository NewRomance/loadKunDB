import org.apache.commons.lang3.time.DateUtils;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DBUtil {
    static IConnectionPool connectionPool;

    public DBUtil(String dbfilepath){
        this.connectionPool = ConnectionPoolImpl.getInstance(dbfilepath);
    }

    public static boolean insertBatch(String sql, List records, Connection conn, List<String> columnTypes){
        boolean flag = true;
        PreparedStatement preStmt = null;
        try  {
            preStmt=conn.prepareStatement(sql);
            conn.setAutoCommit(false);
            int[] affectedNums;
            for(int i = 0; i < records.size(); i++){
                String[] record = (String[])(records.get(i));
                for(int j = 0 ; j < record.length; j++){
                    String colType = columnTypes.get(j);
                    String colValue = record[j];
                    if(colType.equals("DATE")|| colType.equals("DATETIME"))
                    {
                        if(colValue != null)
                            colValue = formatDateOrDateTime(colValue);
                    }
                    preStmt.setObject(j+1,colValue);
                }
                preStmt.addBatch();
            }
            affectedNums=preStmt.executeBatch();
            /**
             * check affectedNums
             * */
            if(affectedNums==null||affectedNums.length==0){
                flag = false;
            }
            for(int i = 0; i<affectedNums.length; i++){
                if(affectedNums[i] <= 0){
                    flag = false;
                    break;
                }
            }
            conn.commit();
        } catch (SQLException e) {
            System.out.println("ERROR: errorCode:"+e.getErrorCode()+" sqlState:"+e.getSQLState()+" errorMsg:"+e.getMessage());
            flag = false;
            e.printStackTrace();
        } finally {
            if (preStmt != null) try {
                preStmt.clearBatch();
                preStmt.close();
            } catch (SQLException e) {
                System.out.println("ERROR: fail to close prepareStatement");
                e.printStackTrace();
            }
        }
        return flag;
    }

    public static String formatDateOrDateTime(String dateStr){
        String[] possibleDateFormats =
                {
                        "yyyy-MM-dd",
                        "yyyy-MM-dd HH:mm:ss",
                        "yyyy-MM-dd HH:mm:ss.xxxxxx",
                     //   "yyyyMMdd",
                    //    "yyyy/MM/dd",
                      //  "yyyy/MM/dd HH:mm:ss",
                     //   "yyyy MM dd",
                        "\"yyyy-MM-dd\"",
                        "\"yyyy-MM-dd HH:mm:ss\"",
                        "\"yyyy-MM-dd HH:mm:ss.xxxxxx\""
                      //  "\"yyyyMMdd\"",
                      //  "\"yyyy/MM/dd\"",
                      //  "\"yyyy/MM/dd HH:mm:ss\"",
                       // "\"yyyy MM dd\"",
                };
      //  SimpleDateFormat newDf = new SimpleDateFormat("yyyy-MM-dd");//设置日期格式
//        if(dateType.equals("DATETIME")) {
//            newDf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        }
        try{
            DateUtils.parseDate(dateStr, possibleDateFormats);
        }catch (java.text.ParseException e){
            System.out.printf("ERROR: invalid dateTime type, value = %s.", dateStr);
            e.printStackTrace();
        }
        return dateStr;
    }

    public String createPrepareStatement(String tableName, List<String> tableColumns){
        //prepared statement sql
        String preStmtStr = "insert into "+tableName+"(";
        int columnsSize = tableColumns.size();
        for(int i = 0; i< columnsSize;i++){
            preStmtStr+=tableColumns.get(i);
            if( i != columnsSize-1){
                preStmtStr+=", ";
            }
        }
        preStmtStr+=") values(";
        for(int i = 0; i< columnsSize;i++){
            preStmtStr+="?";
            if( i != columnsSize-1){
                preStmtStr+=", ";
            }
        }
        preStmtStr+=")";
        return preStmtStr;
    }

    public ArrayList<String> getColumnNamesAndTypes(String tableName, List<String> columns, Connection conn){
        System.out.printf("tableName is %s\n", tableName);
        ArrayList<String> columnTypes = new ArrayList<String>();
        try{
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet cols = dbmd.getColumns(null,null, tableName,null);
            while(cols.next()){
                System.out.printf("type name is %s, column name is %s", cols.getString("TYPE_NAME"), cols.getString("COLUMN_NAME"));
                columnTypes.add(cols.getString("TYPE_NAME"));
                columns.add(cols.getString("COLUMN_NAME"));
            }
        }catch(SQLException e){
            e.printStackTrace();
        }
        return columnTypes;
    }

    public static Connection getConnection(){
        return connectionPool.getConnection();
    }

    public static void freeConnectionPool(){
        connectionPool.freeLocalConnection();
    }

    public static void destroyConnectionPool(){
        connectionPool.destroy();
    }
}
