import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionPoolImpl implements IConnectionPool{

    private String username;
    private String password;
    private String url;
    private String driver;

    private Integer maxSize;
    private Integer initSize;
    private long timeOut;

    //连接总数
    private AtomicInteger totalSize = new AtomicInteger(0);
    //空闲的连接
    private List<Connection> freeConnections = new Vector<Connection>(); //Vector类线程安全
    //已经被使用的连接
    private List<Connection> activeConnections = new Vector<Connection>();
    //存储当前线程的连接， 事务控制的关键
    private ThreadLocal<Connection> localConnection = new ThreadLocal<Connection>(){

        /**
         * 第一次调用get()方法时执行
         * @return
         */
        @Override
        protected Connection initialValue() {
            try {
                return connect();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public void remove() {
            Connection connection = get();
            activeConnections.remove(connection);
            freeConnections.add(connection);
            super.remove();
        }
    };

    private static ConnectionPoolImpl instance;

    public ConnectionPoolImpl(String dbfilepath) {
        loadConfig(dbfilepath);
        init();
    }

    private void init() {
        try {
            for(int i=0;i < initSize;i++){
                freeConnections.add(newConnection());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static ConnectionPoolImpl getInstance(String dbfilepath) {
        synchronized (ConnectionPoolImpl.class) {
            if (instance == null) {
                synchronized (ConnectionPoolImpl.class) {
                    if(instance == null) {
                        instance = new ConnectionPoolImpl(dbfilepath);
                    }
                }
            }
        }
        return instance;
    }


    public synchronized Connection getConnection() {
        return localConnection.get();
    }


    public void freeLocalConnection() {
        localConnection.remove();
       // System.out.println("INFO: "+Thread.currentThread().getName() + "释放了一个连接");

    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        destroy();
    }

    public synchronized void destroy() {
        try {
            for(int i = 0; i < freeConnections.size(); i++){
                Connection connection = freeConnections.remove(i);
                connection.close();
            }
            freeConnections = null;

            for(int i = 0; i < activeConnections.size(); i++){
                Connection connection = activeConnections.remove(i);
                connection.close();
            }
            activeConnections = null;

            System.out.println("INFO: all connections are freed.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public synchronized void status() {
        System.out.println("当前连接池总连接数为: " + totalSize.get() + " , 空闲连接数为：" + freeConnections.size() + "使用中的连接数为：" + activeConnections.size());
    }

    private synchronized Connection connect() throws SQLException {
        // 判断有没有闲置的连接
        if(freeConnections.size() > 0) {
            //如果有闲置连接，直接拿第一个
            Connection connection = freeConnections.get(0);
            freeConnections.remove(0);
            //连接可用，返回；不可用，继续拿
            if (isValid(connection)) {
                activeConnections.add(connection);
                return connection;
            } else {
                return connect();
            }
        } else {
            //没有闲置连接， 判断当前连接池是否饱和
            if(totalSize.get() == maxSize) {
                //如果饱和，等待， 继续获取
                try {
                    wait(timeOut);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return connect();
            } else {
                //没有饱和，新建一个连接
                Connection connection = newConnection();
                if(connection != null) {
                    activeConnections.add(connection);
                    return connection;
                } else {
                    throw new SQLException();
                }
            }
        }
    }

    private synchronized Connection newConnection() throws SQLException {
        try {
            Class.forName(this.driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection connection =  DriverManager.getConnection(url, username, password);
        totalSize.incrementAndGet();
        return connection;
    }

    private boolean isValid(Connection connection) {
        try {
            return connection != null && !connection.isClosed();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private void loadConfig(String dbfilepath){
        //读取配置文件
        InputStream in = null;
        Properties p = new Properties();
        try {
            in = new BufferedInputStream(new FileInputStream(dbfilepath));
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
        this.username = p.getProperty("jdbc.username");
        this.password = p.getProperty("jdbc.password");
        this.url = p.getProperty("jdbc.url");
        this.driver = p.getProperty("jdbc.driver");

        this.maxSize = Integer.valueOf(p.getProperty("noob.maxSize","10"));
        this.initSize = Integer.valueOf(p.getProperty("noob.initSize","5"));
        this.timeOut = Long.valueOf(p.getProperty("noob.timeOut","200"));

    }
}