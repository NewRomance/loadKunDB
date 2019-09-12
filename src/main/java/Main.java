import com.transwarp.loadkundb.common.CommonReader;
import com.transwarp.loadkundb.read.TextReader;
import com.transwarp.loadkundb.util.DBUtil;

import java.util.Scanner;

public class Main {
        public static void main(String[] args) {
            //get params from command line
            //args[0]:filePath,csv/xls/xlsx, args[1]:dbfilePath, args[2]:batchSize
            int length = args.length;
            if (length == 3) {
              //  String filePath = "/home/transwarp/messages3.csv";
  //              String dbfilepath = "/home/transwarp/jdbc.properties";
 //               String filePath = "/home/transwarp/文档/people2.txt";
//                String dbfilepath = "/home/transwarp/IdeaProjects/loadKunDB/src/main/resources/jdbc.properties";
              //  int batchSize = 4;
                String filePath = args[0];
                String dbfilepath = args[1];
                int  batchSize = Integer.parseInt(args[2]);
                if(batchSize <= 0){
                    System.out.println("batchSize should not be negative or zero.");
                    return;
                }
                // get the fileInfo
                int startIndex = filePath.lastIndexOf('/');
                int endIndex = filePath.lastIndexOf('.');
                String filename = filePath.substring(startIndex + 1, endIndex);
                final String fileType = filePath.substring(endIndex + 1);

                //get the fileReader of the according type.
                CommonReader commonReader = new CommonReader();
                if(fileType.equals("csv")||fileType.equals("txt")){
                    Scanner in =new Scanner(System.in);
                    System.out.println("please input the separator of each row in this file:");
                    String separator = in.nextLine();
                    while(separator.equals("")){
                        System.out.println("separator should not be empty string, please input the separator!.");
                        separator = in.nextLine();
                    }
                    commonReader = new TextReader(filePath,filename,batchSize,separator);

                }else{
                    System.out.println("not supported yet.");
                    return;
                }
                final DBUtil dbutil = new DBUtil(dbfilepath);
                commonReader.readAndHandle(dbutil);
            } else {
                System.out.println("Params are needed.");
           }
        }
}
