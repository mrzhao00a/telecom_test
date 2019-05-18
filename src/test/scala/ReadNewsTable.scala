import java.util.{Date, Properties}
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object ReadNewsTable {
  def main(args: Array[String]): Unit = {
    val starttimeZ = System.currentTimeMillis()
    //spark初始化
    val sp_conf = new SparkConf()
    sp_conf.set("spark.shuffle.consolidateFiles", "true")
    val ss = SparkSession.builder().config(sp_conf).master("local[*]").appName("test").enableHiveSupport().getOrCreate()
    val sc = ss.sparkContext
    sc.setLogLevel("warn")

    //hbase配置初始化
    val h_conf = HBaseConfiguration.create()
    h_conf.set("hbase.zookeeper.quorum", "tjnw-bg-pe-02,tjnw-bg-pe-04,tjnw-bg-pe-05") //tjnw-bg-pe-02,tjnw-bg-pe-04,tjnw-bg-pe-05
    h_conf.set("hbase.zookeeper.property.clientPort", "2181") //2181
    h_conf.setInt("hbase.client.scanner.timeout.period", 600000)
    h_conf.setInt("hbase.client.operation.timeout", 300000)
    h_conf.setInt("hbase.rpc.timeout", 600000)
    h_conf.set(TableInputFormat.SCAN_COLUMN_FAMILY,"base")


    //创建方法通过h_conf指定表名和列限定符来提取相应的字段及数据返回一个RDD
    def getHbaseConf(tn :String,cn:String,startRow:String,endRow :String) ={
      h_conf.set(TableInputFormat.INPUT_TABLE,tn)
      h_conf.set(TableInputFormat.SCAN_COLUMNS,cn)
      if(StringUtils.isNotEmpty(startRow))  h_conf.set(TableInputFormat.SCAN_ROW_START,startRow)

      if(StringUtils.isNotEmpty(endRow))  h_conf.set(TableInputFormat.SCAN_ROW_STOP,endRow)

      h_conf
    }
    def getColumValue(result :Result,columName :String)={
      StringUtils.defaultIfEmpty(Bytes.toString(result.getValue(Bytes.toBytes("base"),Bytes.toBytes(columName))),"")
    }

       val newsRDDs = sc.newAPIHadoopRDD(getHbaseConf("nm_news","base:bloggerId,base:mediaType,base:domain_pri,base:domain_sec","1557244800000","1557244920000"),classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result]).map(
      m=>{
        val mediaType = getColumValue(m._2,"mediaType")
        if ("wb,wc,app".contains(mediaType))
//          (mediaType+"-"+getColumValue(m._2,"bloggerId") )
         Seq (mediaType+"-"+getColumValue(m._2,"bloggerId") )

        else
          Seq(mediaType+"-"+getColumValue(m._2,"domain_pri"),mediaType+"-"+getColumValue(m._2,"domain_sec"))

      }).flatMap(f=>f).map(m=>(m,1)).distinct() //神奇的下划线有坑啊

//    val newsRDD = newsRDDs.reduceByKey(_+_)
//        println(newsRDD.count())//106673条,耗时1000秒左右1088

    def getMediaRDD(sc:SparkContext,tn:String)={
      sc.newAPIHadoopRDD(getHbaseConf(tn,"base:bloggerId,base:mediaName,base:mediaType",null,null),classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result]).map(
        m=>{
          (getColumValue(m._2,"mediaType")+"-"+getColumValue(m._2,"bloggerId"),(getColumValue(m._2,"mediaId"),getColumValue(m._2,"mediaName"),getColumValue(m._2,"mediaType")))
        }
      )
    }
    val tables = Array("nm_media_wc", "nm_media_wb", "nm_media_app", "nm_media_bbs", "nm_media_new")
    val rdds = ArrayBuffer.empty[RDD[(String,(String,String,String))]]//empty 意思创建一个空的可变集合
    for (t <- tables){
      rdds.append(getMediaRDD(sc,t))
    }
    val allMediaRDD = sc.union(rdds)
//      .take(100)
//      .foreach(println)
//信源RDD和稿件RDD进行左外连接，取出匹配为空的 信源（id ，type，name）

    val resultRDD =  allMediaRDD.leftOuterJoin(newsRDDs).filter(f=>f._2._2 isEmpty)

    // RDD[(String, ((String, String, String), Option[Int]))]
    resultRDD.foreachPartition(rdd=>{
      rdd.foreach(println(_))//匹配结果
    })
    import ss.implicits._
    val yesterday = DateFormatUtils.format(DateUtils.addDays(new Date(),-1),"yyyy-MM-dd")
    val rowRDD = resultRDD.map(f=>({
//      val s = f._1.split("-")
//      Row(yesterday,s(0),s(1),f._2._1)  //(wb-678,(hbase,None))
      Row(yesterday,f._2._1._1,f._2._1._2,f._2._1._3)  //(wb-678,(hbase,None)) //利用元祖效率更高！
    }))
//    rowRDD.foreach(println(_))
    val URL = "jdbc:mysql://tjnw-dbvip-pe-02:3306/logsniper?useUnicode=true&characterEncoding=utf8&useSSL=false"
    val UP = new Properties()
    UP.setProperty("user", "logsniper")
    UP.setProperty("password", "BGhJe4ZvsIQkE21J")
    UP.setProperty("driver", "com.mysql.jdbc.Driver")

    val schema = StructType(StructField("date", StringType) :: StructField("media_type", StringType) :: StructField("media_id", StringType) :: StructField("media_name", StringType) :: Nil)
    ss.createDataFrame(rowRDD,schema).write.mode("append").jdbc(URL,"tjnw_dc_media_no_data",UP)

    println("恭喜，数据导入MySQL成功，请验证")







    val endtimeZ = System.currentTimeMillis()
    val durationtimeZ = endtimeZ - starttimeZ
    println("总时间"+durationtimeZ/1000 +"s")
    sc.stop()
    ss.stop()
    //news
//    (wc-wuhuxianfeng,1)
//    (wc-ok66txw,1)
//    (wc-Qnnrrxx,1)
//    (wc-lmzt100,1)
//    (wc-hbsd101,1)
//    (wc-jingd81,1)
//    (wb-1841391791,1)
//    (wc-qyaskgjyc2013,1)

    //信源
//    (wc-twbpp2015,台湾白婆婆酵素)
//    (wc-nmg535,精选情歌666)
//    (wc-qqrzzl,不可靠消息)
//    (wc-fhyc8316888,都匀凤凰影城)
//    (wc-wxjj112,玩笑家)
//    (wc-p18863776466,金毛犬价格 哈士奇犬价格)
//    (wc-kashi0557,classykiss宿州)
//    (wc-kfztc8,康复直通车)
//    2分钟数据跑了22分钟







  }
}
