import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseConf {
    public static Configuration readData3(String tableName) {
        //初始化hbase的链接
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "tjnw-bg-pe-02,tjnw-bg-pe-04,tjnw-bg-pe-05");//tjnw-bg-pe-02,tjnw-bg-pe-04,tjnw-bg-pe-05
        conf.set("hbase.zookeeper.property.clientPort", "2181");//2181
        //输入表表名
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        conf.setInt("hbase.client.scanner.timeout.period",500000);
        conf.setInt("hbase.client.operation.timeout",300000);
        conf.setInt("hbase.rpc.timeout",500000);
        conf.set(TableInputFormat.SCAN_COLUMN_FAMILY,"base");
//        conf.set(TableInputFormat.SCAN_COLUMNS,"base:bloggerId,base:mediaName");

        return conf;
    }

    public static String isNull(String data) {
        if (data == null) {
            return "";
        }
        return data;
    }
}
