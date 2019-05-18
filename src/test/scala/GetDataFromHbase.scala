import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object GetDataFromHbase {
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create
    conf.set("hbase.zookeeper.quorum", "tjnw-bg-pe-02,tjnw-bg-pe-04,tjnw-bg-pe-05") //tjnw-bg-pe-02,tjnw-bg-pe-04,tjnw-bg-pe-05
    conf.set("hbase.zookeeper.property.clientPort", "2181") //2181
    conf.setInt("hbase.client.scanner.timeout.period", 600000)
    conf.setInt("hbase.client.operation.timeout", 300000)
    conf.setInt("hbase.rpc.timeout", 600000)
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "base") // 列簇指定
    conf.set(TableInputFormat.SCAN_COLUMNS,"") // 列簇指定

    def getHbaseConf(tableName: String, startRow: String, endRow: String) = { //初始化hbase的链接
      conf.set(TableInputFormat.INPUT_TABLE, tableName)
      if (StringUtils.isNotEmpty(startRow)) {
        conf.set(TableInputFormat.SCAN_ROW_START, startRow)
      }
      if (StringUtils.isNotEmpty(endRow)) {
        conf.set(TableInputFormat.SCAN_ROW_STOP, endRow)
      }
      conf
    }

    val spark =new SparkConf().setMaster("local[*]").setAppName("Read")
    val sc = new SparkContext(spark)

    val newRdd = sc.newAPIHadoopRDD(getHbaseConf("nm_news","1557802474000","1557802475000")
      ,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result]).map(m=>{
//      (StringUtils.defaultIfEmpty(Bytes.toString(m._2.getRow),""),StringUtils.defaultIfEmpty(Bytes.toString(m._2.value()),""))

    })
    println(newRdd.count())
    newRdd.saveAsTextFile("E:\\input\\new.txt")





  }

}
