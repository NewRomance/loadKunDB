import sun.misc.Signal;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {
    private static String filePath;
    private static String dbfilepath;
    private static int batchSize;
    private static int threadsNum;
    private static boolean hasHeaders;
    private static String encoding;
    private static String separator;
    private static String enclosedChar;
    private static String failedFileDir;
    private static int maximumFailedFilesCount;

    public static String getOSSignalType(){
        return System.getProperties().getProperty("os.name").toLowerCase().startsWith("win") ? "INT" : "USR2";
    }

    private static void loadConfig(String initFilePath){
        /**读取配置文件
         * 配置文件路径为英文
         * */
        InputStream in = null;
        Properties p = new Properties();
        try {
            in = new BufferedInputStream(new FileInputStream(initFilePath));
            p.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        filePath= p.getProperty("params.filePath");
        dbfilepath = p.getProperty("params.dbfilepath");
        batchSize = Integer.valueOf(p.getProperty("params.batchSize"));
        threadsNum = Integer.valueOf(p.getProperty("params.threadsNum"));
        hasHeaders = Boolean.valueOf(p.getProperty("params.hasHeaders"));
        encoding = p.getProperty("params.encoding");
        separator = p.getProperty("params.separator");
        enclosedChar = p.getProperty("params.enclosedChar");
        failedFileDir = p.getProperty("params.failedFileDir");
        maximumFailedFilesCount = Integer.valueOf(p.getProperty("params.maximumFailedFilesCount"));

        System.out.println("-------------------Config INFO------------------");
        System.out.println("数据文件路径: "+filePath);
        System.out.println("数据库配置文件路径： "+dbfilepath);
        System.out.println("批量处理记录的数目: "+batchSize);
        System.out.println("启用的线程数: "+threadsNum);
        System.out.println("数据文件是否带表头: "+hasHeaders);
        System.out.println("数据文件的编码: "+encoding);
        System.out.println("每行记录中数据之间的分隔符: "+separator);
        System.out.println("每行记录中每个数据的包围符号: "+enclosedChar);
        System.out.println("批量插入失败的文件目录： "+failedFileDir);
        System.out.println("最大允许的批量插入失败的文件数目： "+maximumFailedFilesCount);
        System.out.println("------------------------------------------------");
    }
    public static void main(String[] args) {
        /**get params from command line.
         * args[0]:filePath,csv/xls/xlsx, args[1]:dbfilePath, args[2]:batchSize, args[3]:threadsNum, args[4]:hasHeaders, args[5]:UTF-8
         */
        if(args.length != 0) {
            /**read the config file*/
            Main.loadConfig(args[0]);
           // Main.loadConfig("
            //
            // ");
            /**
             * make java process exit elegantly, when meet System.exit(0) or other interrupt signals such Crl+C on windows.
             * */
//            Signal sig = new Signal(Main.getOSSignalType());
//            ShutdownHandler shutdownHandler = new ShutdownHandler();
//            Signal.handle(sig, shutdownHandler);

            /** get the fileInfo */
            int startIndex = filePath.lastIndexOf('/');
            int endIndex = filePath.lastIndexOf('.');
            String filename = filePath.substring(startIndex + 1, endIndex);
            final String fileType = filePath.substring(endIndex + 1);

            /** get the fileReader of the according type. */
            CommonReader commonReader = new CommonReader();
            if (fileType.equals("csv") || fileType.equals("txt")) {
                commonReader = new textReader(filePath, filename, batchSize, threadsNum, separator, enclosedChar, hasHeaders, encoding,failedFileDir, maximumFailedFilesCount);
            } else {
                System.out.println("not supported yet.");
                return;
            }
            final DBUtil dbutil = new DBUtil(dbfilepath);
            commonReader.readAndHandle(dbutil);
        }else{
            System.out.println("ERROR: params are needed.");
        }
    }
}