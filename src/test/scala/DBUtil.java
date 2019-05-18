import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.sql.SQLException;
import java.util.Map;

public class DBUtil {
    private static Logger logger = Logger.getLogger(DBUtil.class);

    private static HikariDataSource dataSource;
    private static HikariDataSource getInstance() {
        /**
         * 连接池配置
         */
        if (dataSource == null) {
            HikariConfig config = new HikariConfig();
            config.setDriverClassName(CommonUtil.config.getString("DB_DRIVE"));//com.mysql.cj.jdbc.Driver
            config.setJdbcUrl(CommonUtil.config.getString("DB_URL"));//?useUnicode=true&characterEncoding=utf8&useSSL=false
            config.setUsername(CommonUtil.config.getString("DB_USERNAME"));
            config.setPassword(CommonUtil.config.getString("DB_PASSWORD"));
            //池中最小空闲链接数量
            config.setMinimumIdle(10);
            //池中最大链接数量
            config.setMaximumPoolSize(100);
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            // 设置连接超时为8小时
            config.setConnectionTimeout(8 * 60 * 60);
            config.setAutoCommit(true);
            dataSource = new HikariDataSource(config);
        }
        return dataSource;
    }


    /**
     * 获取数据库连接
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() {
        /*
         * 通过连接池获取一个空闲连接
         */
        try {
            return getInstance().getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            getInstance().resumePool();
            return null;
        }
    }

    /**
     * 关闭数据库连接
     */
    public static void close(Statement pst, Connection conn) {
        try {
            if (pst != null) {
                pst.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pst = null;
            conn = null;
        }
    }
    public static void close(ResultSet rs, Statement pst, Connection conn) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (pst != null) {
                pst.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rs = null;
            pst = null;
            conn = null;
        }
    }



    //batch
    public static  int[] executeBatch(String sql, List<Object[]> paramsList) throws Exception{


        int[] lines = null;
        Connection connection = null;
        PreparedStatement pstmt = null;

        //try {
        // 从连接池里面把连接拿到
        connection = getConnection();
        connection.setAutoCommit(false); // 关闭了自动提交
        pstmt = connection.prepareStatement(sql);

        // 把 List<Object[]> paramsList 参数添加
        for (Object[] params : paramsList) {

            for (int i = 0; i < params.length; i++) {

                pstmt.setObject(i + 1, params[i]);
            }
            pstmt.addBatch();
        }
        lines = pstmt.executeBatch();
        connection.commit();

        //} catch (Exception e) {
        // e.printStackTrace();
        // logger.error(e.getMessage());
        //} finally {
        //  close(pstmt, connection);
        //}
        return lines;
    }


    public static int[] insterResult(HashMap<String,String> map ) {
        String sql = "INSERT INTO tjnw_dc_media_no_data(date,media_type,media_id,media_name) VALUES(?,?,?,?)";
        ArrayList<Object[]> paramsList = new ArrayList<>();
        String  date= new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        for (Map.Entry<String,String>entry : map.entrySet()) {
            String[] split = entry.getKey().split("-");
            Object[] params = {
                    date,//日期
                    split[0],//meidia_typey-media_id
                    split[1],//media_type
                    entry.getValue()// media_name
            };
            paramsList.add(params);
        }
        try{
            int[] ints = DBUtil.executeBatch(sql, paramsList);//插入mysql
            return ints;
        }catch (Exception e){
            System.out.println(e.toString());

            System.exit(0);
        }
        int [] test = new int[2];
        return test;
    }
    public static void batchInsertResult(Map<String,String> input, int size)throws Exception {
       HashMap<String,String>batchList=new HashMap<>();
        for (Map.Entry<String,String> entry: input.entrySet()) {
            batchList.put(entry.getKey(),entry.getValue());
            if (batchList.size() == size) {
                insterResult(batchList);
                batchList.clear();
            }
        }
        if(batchList.size()>0)
            insterResult(batchList);
    }
}
