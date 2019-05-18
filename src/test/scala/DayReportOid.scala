import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.{SparkConf, SparkContext}


object DayReportOid {
  def main(args: Array[String]): Unit = {
//    args[0] 程序名称，args[1]表名（Hbase六张） args[2]

    val spark = new SparkConf().setAppName("ReadHbaseData").setMaster("local[*]")
    val sc = new SparkContext(spark)
    sc.setLogLevel("warn")
    //读取nm_media_wc表 只提取需要字段 blogerId 和 mediaName  app bbs wb wc  new
//    val wcData = makeRddMedia(sc,"nm_media_wc","base","bloggerId","mediaName")
    var wcData = sc.newAPIHadoopRDD(HbaseConf.readData3("nm_media_wc"), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map(
      m => {
//        val bloggerId = HbaseConf.isNull(Bytes.toString(m._2.getValue(Bytes.toBytes("base"), Bytes.toBytes("bloggerId"))))
        val bloggerId = fieldsExtract(m._2,"base","bloggerId")
        val mediaName = fieldsExtract(m._2,"base","mediaName")
//        val mediaName = HbaseConf.isNull(Bytes.toString(m._2.getValue(Bytes.toBytes("base"), Bytes.toBytes("mediaName"))))

        (bloggerId, mediaName)
//        (xxxzw100,小学写作)
      })
    //.take(1).foreach(println)
  //读取nm_media_wb表
  var wbData = sc.newAPIHadoopRDD(HbaseConf.readData3("nm_media_wb"), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    .map(
      m => {
        val bloggerId = fieldsExtract(m._2,"base","bloggerId")
        val mediaName = fieldsExtract(m._2,"base","mediaName")
        (bloggerId, mediaName)
      })
    //读取nm_media_app表
    var appData = sc.newAPIHadoopRDD(HbaseConf.readData3("nm_media_app"), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map(
        m => {
          val bloggerId = fieldsExtract(m._2,"base","bloggerId")
          val mediaName = fieldsExtract(m._2,"base","mediaName")
          (bloggerId, mediaName)
        })
    //读取nm_media_bbs表
    var bbsData = sc.newAPIHadoopRDD(HbaseConf.readData3("nm_media_bbs"), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map(
        m => {
          val bloggerId = fieldsExtract(m._2,"base","bloggerId")
          val mediaName = fieldsExtract(m._2,"base","mediaName")
          (bloggerId, mediaName)
        })
    //读取nm_media_new表
    var newData = sc.newAPIHadoopRDD(HbaseConf.readData3("nm_media_new"), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map(
        m => {
          val bloggerId = fieldsExtract(m._2,"base","bloggerId")
          val mediaName = fieldsExtract(m._2,"base","mediaName")
          (bloggerId, mediaName)
        })


//    合并所有结果

    val wc_wb_RDD = wcData.union(wbData)
    val app_bbs_RDD = appData.union(bbsData)
    val cbab_RDD= wc_wb_RDD.union(app_bbs_RDD)

    val allMediaRDD = cbab_RDD.union(newData)
    allMediaRDD.take(1000).foreach(println(_))


    // 提取 稿件表数据字段，（domain_pri，domain_sec，mediaName，bloggerId） newsDadaRDD
    val tablename = "nm_news"
    val hbaseConf = HbaseConfWithFilter.readData3(tablename,"1557072000000","1557244800000")
    var newsDataRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
        .map(m=>{
      val mediaType = HbaseConf.isNull(Bytes.toString(m._2.getValue(Bytes.toBytes("base"), Bytes.toBytes("mediaType"))))
      if ("wb,wc,app".contains(mediaType)){Seq(HbaseConf.isNull(Bytes.toString(m._2.getValue(Bytes.toBytes("base"), Bytes.toBytes("bloggerId")))))}
      else Seq(HbaseConf.isNull(Bytes.toString(m._2.getValue(Bytes.toBytes("base"), Bytes.toBytes("domain_pri")))), HbaseConf.isNull(Bytes.toString(m._2.getValue(Bytes.toBytes("base"), Bytes.toBytes("domain_sec")))))
    }).flatMap( f=> f).distinct().map(x =>(x,1))



    //allMediaDataRDD 和 newsDadaRDD 进行左外join 匹配没有 发稿件的账户ID
    val resultRDD = allMediaRDD.leftOuterJoin(newsDataRDD).filter(x=>(x._2._2 isEmpty))






    //最终结果存入MySQl

  }
  //提取 字段返回一个RDD
  def fieldsExtract(value :Result , cf :String, cn :String) :String ={
     HbaseConf.isNull(Bytes.toString(value.getValue(Bytes.toBytes(cf), Bytes.toBytes(cn))))
  }
  def makeRddMedia (sc: SparkContext,table_name:String,cf :String,blogger_id: String,media_name: String):Any= {
    sc.newAPIHadoopRDD(HbaseConf.readData3(table_name), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map(
        m => {
          val bloggerId = fieldsExtract(m._2, cf, blogger_id)
          val mediaName = fieldsExtract(m._2, cf, media_name)
          (bloggerId, mediaName)
        })
  }


}
