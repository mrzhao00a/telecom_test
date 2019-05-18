package com.taiji.xmt.jobs


import java.util.{Date, Properties}
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object DayReport {
  def main(args: Array[String]): Unit = {
    //    args[0] 程序名称，args[1]表名（Hbase六张） args[2]
    //    val appName = args(0)
    //    val parallelism = args(1)
    //    val startRowKey = args(2)
    //    val endRowKey = args(3)
    //一、spark，Hbase，mysql 初始化的相关配置
    //Hbased 配置文件的相关固定设置
    val conf = HBaseConfiguration.create
    conf.set("hbase.zookeeper.quorum", "tjnw-bg-pe-02,tjnw-bg-pe-04,tjnw-bg-pe-05") //tjnw-bg-pe-02,tjnw-bg-pe-04,tjnw-bg-pe-05
    conf.set("hbase.zookeeper.property.clientPort", "2181") //2181
    conf.setInt("hbase.client.scanner.timeout.period", 600000)
    conf.setInt("hbase.client.operation.timeout", 300000)
    conf.setInt("hbase.rpc.timeout", 600000)
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "base") // 列簇指定
    conf.set(TableInputFormat.SCAN_COLUMNS, "base:bloggerId,base:mediaName") // 指定业务所需要的列，其他列舍弃， 提高效率，这行可以不用要，因为需要参数指定


    // spark相关配置
    val spark1 = new SparkConf()
      .set("set spark.driver.allowMultipleContexts ", " true") //以下是spark的优化策略配置
      .set("spark.network.timeout", "20000")
      .set("spark.scheduler.mode", "FIFO")
      .set("spark.driver.maxResultSize", "10g")
      .set("spark.shuffle.memoryFraction", "0.8")
      .set("spark.storage.memoryFraction", "0.2")
      .set("spark.rdd.compress", "true")
      .set("spark.default.parallelism", "100") //并行度
      .set("spark.shuffle.consolidateFiles", "true") //shuffle端的文件聚合
      .set("spark.io.compression.codec", "lz4")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.referenceTracking", "false")
      .set("spark.kryoserializer.buffer.max", "2000m")
      .set("spark.kryoserializer.buffer", "1024m")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.client.Put]))
    //    val sc = new SparkContext(spark1)  错误！程序中不能运行两个  spark入口程序

    val ss = SparkSession.builder().appName("dayReport").master("local[*]").config(spark1).enableHiveSupport().getOrCreate() //sparkSQL 程序入口
    val sc = ss.sparkContext //离线任务处理程序入口必须通过sparkseession来创建
        sc.setLogLevel("warn")
    //   hbase动态配置文件选项的设置
    def getHbaseConf(tableName: String, columns: String, startRow: String, endRow: String) = { //初始化hbase的链接
      conf.set(TableInputFormat.INPUT_TABLE, tableName)
      if (StringUtils.isNotEmpty(columns)) {
        conf.set(TableInputFormat.SCAN_COLUMNS, columns)
      }
      if (StringUtils.isNotEmpty(startRow)) {
        conf.set(TableInputFormat.SCAN_ROW_START, startRow)
      }
      if (StringUtils.isNotEmpty(endRow)) {
        conf.set(TableInputFormat.SCAN_ROW_START, endRow)
      }
      conf
    }

    //提取 字段返回一个RDD
    def makeRddMedia(sc: SparkContext, table_name: String): RDD[(String, String)] = {
      sc.newAPIHadoopRDD(getHbaseConf(table_name, null, null, null), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
        .map(m => {
          (getColumnValue(m._2, "mediaType") + "-" + getColumnValue(m._2, "bloggerId"), getColumnValue(m._2, "mediaName"))
        })
    }

    def getColumnValue(result: Result, column: String) = { //返回值什么也不不写可以自己进行类型推断
      StringUtils.defaultIfEmpty(Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes(column))), "")//defaultIfEmpty 处理头疼的 空指针异常
    }

    val tables = Array("nm_media_wc", "nm_media_wb", "nm_media_app", "nm_media_bbs", "nm_media_new")
    val rddSeq = ArrayBuffer.empty[RDD[(String, String)]] //收集多个RDD
    for (s <- tables) {
      rddSeq.append(makeRddMedia(sc, s)) //将解析出来的RDD追加到ArrayBuffer中
    }
    val allMediaRDD = sc.union(rddSeq) //合并所有RDD

    //    println(allMediaRDD.count) //60210

    //      .take(100).foreach(println(_))
    // 提取 稿件表数据字段，（domain_pri，domain_sec，mediaName，bloggerId）
    val newsDataRDD = sc.newAPIHadoopRDD(getHbaseConf("nm_news", "base:bloggerId,base:mediaType,base:domain_pri,base:domain_sec", "1557244800000", "1557244920000"), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map(m => {
        val mediaType = getColumnValue(m._2, "mediaType")
        if ("wb,wc,app".contains(mediaType)) {
          Seq(mediaType + "-" + getColumnValue(m._2, "bloggerId"))
        }
        else Seq(mediaType + "-" + getColumnValue(m._2, "domain_pri"), mediaType + "-" + getColumnValue(m._2, "domain_sec"))
      }).flatMap(x => x).map(x => (x, 1)).distinct()

      //      .distinct()大哥仔细点不用消重
      .persist(StorageLevel.MEMORY_AND_DISK)

    //    println(newsDataRDD.count())


    //allMediaDataRDD 和 newsDadaRDD 进行左外join 匹配没有 发稿件的账户ID
    //    import scala.collection.JavaConverters._ scala集合转换成java集合常用  asjava
//    val day = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val yesterday = DateFormatUtils.format(DateUtils.addDays(new Date(),-1),"yyyy-MM-dd")

    val resultRDD = allMediaRDD.leftOuterJoin(newsDataRDD).filter(x => (x._2._2 isEmpty)).map(x => {
      val s = x._1.split("-")
      //      Row(day, x._1.split("_")(0), x._1.split("_")(1), x._2._1)
      Row(yesterday, s(0), s(1), x._2._1)
    })
    //      .map(f => Row(f(0), f(1), f(2), f(3)))  //这里进行ROW类型转化，编译时不会报错，运行或者打包或者build时会报错！
    //      .collect().toMap.asJava
    //      .take(100).foreach(println(_))
    import ss.implicits._ // rdd 转换成 df 需要隐式转换
    val schema = StructType(StructField("date", (StringType)) :: StructField("media_type", (StringType)) :: StructField("media_id", (StringType)) :: StructField("media_name", (StringType)) :: Nil) //定义表结构
    //    最终结果存入MySQl
    //mysql 相关配置
    val URL = "jdbc:mysql://tjnw-dbvip-pe-02:3306/logsniper?useUnicode=true&characterEncoding=utf8&useSSL=false"
    val UP = new Properties()
    UP.setProperty("user", "logsniper")
    UP.setProperty("password", "BGhJe4ZvsIQkE21J")
    UP.setProperty("driver", "com.mysql.jdbc.Driver")
    newsDataRDD.unpersist()
    //将rdd转成df 最终结果以追加方式存入MySQl
    ss.createDataFrame(resultRDD, schema).write.mode("append").jdbc(URL, "tjnw_dc_media_no_data", UP)

    sc.stop()
    ss.stop()
  }

}

/*业务逻辑：spark 读取Hbase表中数据,进行逻辑计算，然后将计算结果导入到Mysql中
主要的技术 saprk读取hbase的方式
通过sparksql批量导入mysql
*/
