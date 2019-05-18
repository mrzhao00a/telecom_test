import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.tools.cmd.Property

object Spark2MySQl {
  def main(args: Array[String]): Unit = {
    //一、sparkconf、mysql 初始化设置
    val spark_conf = new SparkConf()
    val ss = SparkSession.builder()
      .config(spark_conf)
      .master("local[*]")
      .appName("s2m")
//      .enableHiveSupport()//开启Hive支持
      .getOrCreate()

    val sc = ss.sparkContext
        sc.setLogLevel("warn")
    val mySQL_conf = new Properties()//用来储存 mysql 用户名 密码 驱动
    mySQL_conf.setProperty("user","root")
    mySQL_conf.setProperty("password","root")
    mySQL_conf.setProperty("driver","com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://localhost:3306/jun?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false&"


    //二、spark读取本地数据
    val accountRDD = sc.makeRDD(Seq(("wb-123","java"),("wb-234","hive"),("wc-456","hadoop"),("wc-678","spark"),("wb-678","hbase"),("wc-789","mysql"),("wc-900","redis")))
    val newsRDD = sc.makeRDD(Seq(Seq("wb-123","wb-234"),Seq("wc-456","wc-678"),Seq("wc-456","wc-678"),Seq("wc-456","wc-678"),Seq("wc-789","wc-900"))).flatMap(f =>(f)).distinct().map(f=>(f,1))
   println("______________________accountRDD___________________________")
    accountRDD.foreach(println(_))
    println("______________________newsRDD___________________________")
    newsRDD.foreach(println(_))
    val resultRDD = accountRDD.leftOuterJoin(newsRDD).filter(f=>(f._2._2 isEmpty))
    println("______________________result___________________________")
    resultRDD.foreach(println(_))
    //三、RDD转化成df
    import ss.implicits._
    val rowRDD = resultRDD.map(f=>({
      val s = f._1.split("-")//这样效率会有些滴，可以考虑使用tuple
      Row(s(0),s(1),f._2._1)  //(wb-678,(hbase,None))
    }))
    println("---------------------------rowRDD-----------------------------")
    rowRDD.foreach(println(_))
    val schema = StructType(StructField("meida_type",(StringType))::StructField("media_id",(StringType))::StructField("media_name",(StringType))::Nil)
      ss.createDataFrame(rowRDD,schema).write.mode("append").jdbc(url,"dayreport",mySQL_conf)
    println("恭喜，数据导入MySQL成功，请验证")
    //四、结果批量写入mySQl

  }

}
