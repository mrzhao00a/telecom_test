import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;


public class HbaseConfNew {
    public static Configuration readData3(String tableName, String start,String stop) {
        //初始化hbase的链接
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "tjnw-bg-pe-02,tjnw-bg-pe-04,tjnw-bg-pe-05");//tjnw-bg-pe-02,tjnw-bg-pe-04,tjnw-bg-pe-05
        conf.set("hbase.zookeeper.property.clientPort", "2181");//2181
        //输入表表名
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        conf.setInt("hbase.client.scanner.timeout.period",200000);
        conf.setInt("hbase.client.operation.timeout",300000);
        conf.setInt("hbase.rpc.timeout",200000);
//        conf.setInt(TableInputFormat.SCAN_ROW_START,start);
//        conf.setInt(TableInputFormat.SCAN_ROW_STOP, stop);


        //过滤器

//        Long time = System.currentTimeMillis();
//        String nowTime = time.toString();
//        Scan scan = new Scan(Bytes.toBytes(maxCreatedTime),Bytes.toBytes(nowTime));
//        scan.setCacheBlocks(false);
//        String scan_str = null;
//        try {
//            scan_str = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray());
//        } catch (IOException e) {
//            return  conf;
//        }
//        conf.set(TableInputFormat.SCAN, scan_str);
        return conf;
    }
}
