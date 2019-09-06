import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {
        public static void main(String[] args) {
            //get params from command line
            //args[0]:filePath,csv/xls/xlsx, args[1]:dbfilePath, args[2]:batchSize
           // int length = args.length;
            //if (length == 3) {
              //  String filePath = "/home/transwarp/messages3.csv";
              //  String dbfilepath = "/home/transwarp/jdbc.properties";
                String filePath = "/home/transwarp/文档/people2.txt";
                String dbfilepath = "/home/transwarp/IdeaProjects/loadKunDB/src/main/resources/jdbc.properties";
                int threadNum = 3;
              //  String filePath = args[0];
              //  String dbfilepath = args[1];
              //  int threadNum = Integer.parseInt(args[2]);

                // get the fileInfo
                int startIndex = filePath.lastIndexOf('/');
                int endIndex = filePath.lastIndexOf('.');
                String filename = filePath.substring(startIndex + 1, endIndex);
                final String fileType = filePath.substring(endIndex + 1);

                //get the fileReader of the according type.
                CommonReader commonReader = new CommonReader();
                if (fileType.equals("csv")) {
                    commonReader = new CsvReader(filePath, filename);
                } else if(fileType.equals("txt")){
                    Scanner in =new Scanner(System.in);
                    System.out.println("please input the separator of each row in this file:");
                    String separator = in.nextLine();
                    while(separator.equals("")){
                        System.out.println("separator should not be empty string, please input the separator!.");
                        separator = in.nextLine();
                    }
                    commonReader = new TxtReader(filePath,filename,separator);

                }else{
                    System.out.println("not supported yet.");
                    return;
                }
                //get a list of table data.
                ArrayList<List> tableDatalist = commonReader.readInFile();
                //get a list of table names.
                ArrayList<String> tableNamelist = commonReader.getTablenamelist();
                //get a list of table columns.
                ArrayList<List> tableColumnslist = commonReader.getTableColumnslist();
                //start threads for transporting each table.
                int i = 0, size = tableDatalist.size();
                final DBUtil dbutil = new DBUtil(dbfilepath);

                while (tableDatalist != null && i < size) {
                    String tableName = tableNamelist.get(i);
                    List records = tableDatalist.get(i);
                    List tableColumns = tableColumnslist.get(i);
                    i++;
                    DBUtil.createAndInsertSql(tableName, tableColumns, records, threadNum);
                }

            //} else {
               // System.out.println("Params are needed.");
            //}
        }
}
