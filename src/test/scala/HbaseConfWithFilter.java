import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseConfWithFilter {
    public static Configuration readData3(String tableName,String stratTime,String stoptime) {
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
        conf.set(TableInputFormat.SCAN_COLUMNS,"base:bloggerId,base:mediaType,base:domain_pri,base:domain_sec");

        //过滤器  指定rowKey区间

//        Long time = System.currentTimeMillis();
//        String nowTime = time.toString();
        Scan scan = new Scan(Bytes.toBytes(stratTime),Bytes.toBytes(stoptime));
        scan.setCacheBlocks(false);
        String scan_str = null;
        try {
            scan_str = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray());
        } catch (IOException e) {
            return  conf;
        }

        conf.set(TableInputFormat.SCAN, scan_str);
        return conf;
    }
}
