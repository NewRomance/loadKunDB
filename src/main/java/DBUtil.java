import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DBUtil {
    static IConnectionPool connectionPool;

    public DBUtil(String dbfilepath){
        this.connectionPool = ConnectionPoolImpl.getInstance(dbfilepath);
    }

    /**
     * 去除分隔符
     * */
    public static String trimStartAndEnd(String value, String enclosedCh){
        if(value != null && value.length() >= 2 && enclosedCh!=null && !enclosedCh.equals("")){
            String start = value.charAt(0)+"";
            int length = value.length();
            String end = value.charAt(length-1)+"";
            if(start.equals(enclosedCh) && end.equals(enclosedCh))
                value=value.substring(1, length-1);
        }
        return value;
    }

    /**
     * 判空函数,
     * 目前认为列值为空的情况
     * 所有类型包括字符串，如果一开始列值就没有enclosedCh包围，则认为是空值；如果enclosedCh为“，列值为“NULL”或者“null”，则认为为空值。
     * */
    public static boolean isNull(String colvalue, String enclosedCh){
        if (colvalue == null || colvalue.equals("")) return true;
        colvalue = trimStartAndEnd(colvalue,enclosedCh);
        if (colvalue.equals("NULL")|| colvalue.equals("null"))
            return true;
        return false;
    }

    public static boolean insertBatch(String sql, List records, String enclosedCh, Connection conn, List<String> columnTypes, int start, int end){
        boolean flag = true;
        PreparedStatement preStmt = null;
        try  {
            preStmt=conn.prepareStatement(sql);
            System.out.println("INFO: start to insert batch data range between ["+start+","+end+")");
            conn.setAutoCommit(false);
            for(int i = 0; i < records.size(); i++){
                String[] record = (String[])(records.get(i));
                for(int j = 0 ; j < record.length; j++){
                    String colValue = record[j];
                    if (isNull(colValue, enclosedCh)) {
                        preStmt.setNull(j+1,0);
                    }else{
                        colValue = trimStartAndEnd(colValue, enclosedCh);
                        preStmt.setObject(j+1,colValue);
                    }
                }
                preStmt.addBatch();
            }
            preStmt.executeBatch();
            /**
             * check affectedNums
             * */
            conn.commit();
        } catch (SQLException e) {
            //System.out.println("ERROR: errorCode:"+e.getErrorCode()+" sqlState:"+e.getSQLState()+" errorMsg:"+e.getMessage());
            flag = false;
            try {
                conn.rollback();
                System.out.println("ERROR: fail to execute transaction(data range between ["+start+","+end+"),)"+"and it will be rollback.");
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            //e.printStackTrace();
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
        System.out.printf("INFO: tableName is %s ", tableName);
        System.out.println();
        ArrayList<String> columnTypes = new ArrayList<String>();
        try{
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet cols = dbmd.getColumns(null,null, tableName,null);
            while(cols.next()){
                //   System.out.printf("type name is %s, column name is %s", cols.getString("TYPE_NAME"), cols.getString("COLUMN_NAME"));
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