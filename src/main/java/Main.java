import java.util.ArrayList;
import java.util.List;

public class Main {
        public static void main(String[] args) {
            //get params from command line
            //args[0]:filePath,csv/xls/xlsx, args[1]:dbfilePath, args[2]:batchNum
            int length = args.length;
            if (length == 3) {
              //  String filePath = "/home/transwarp/messages3.csv";
              //  String dbfilepath = "/home/transwarp/jdbc.properties";
              //  String filePath = "/home/transwarp/文档/people.csv";
              //  String dbfilepath = "/home/transwarp/IdeaProjects/pioKunDB/src/main/resources/jdbc.properties";
              //  int threadNum = 3;
                String filePath = args[0];
                String dbfilepath = args[1];
                int threadNum = Integer.parseInt(args[2]);

                // get the fileInfo
                int startIndex = filePath.lastIndexOf('/');
                int endIndex = filePath.lastIndexOf('.');
                String filename = filePath.substring(startIndex + 1, endIndex);
                final String fileType = filePath.substring(endIndex + 1);

                //get the fileReader of the according type.
                CommonReader commonReader = new CommonReader();
                if (fileType.equals("csv")) {
                    commonReader = new CsvReader(filePath, filename);
                } else {

                    System.out.println("not supported yet.");
                    return;
                }
                //get a list of table data.
                ArrayList<List> tableDatalist = commonReader.readInFile();
                //get a list of table names.
                ArrayList<String> tableNamelist = commonReader.getTablenamelist();
                //get a list of table columns.
                ArrayList<ArrayList<String>> tableColumnslist = commonReader.getTableColumnslist();
                //start threads for transporting each table.
                int i = 0, size = tableDatalist.size();
                final DBUtil dbutil = new DBUtil(dbfilepath);

                while (tableDatalist != null && i < size) {
                    String tableName = tableNamelist.get(i);
                    List records = tableDatalist.get(i);
                    ArrayList<String> tableColumns = tableColumnslist.get(i);
                    i++;
                    DBUtil.createAndInsertSql(tableName, tableColumns, records, threadNum);
                }

            } else {
                System.out.println("Params are needed.");
            }
        }
}
