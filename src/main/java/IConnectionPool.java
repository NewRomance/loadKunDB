import java.sql.Connection;

public interface IConnectionPool {

    /**
     * 获取一个连接
     * @return
     */
    Connection getConnection();

    /**
     * 用完后调用，把连接放回池中，实现复用
     */
    void freeLocalConnection();

    /**
     * 销毁连接池
     */
    void destroy();

    //测试用
    void status();

}