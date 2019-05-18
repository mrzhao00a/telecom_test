import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MapPartitionsWithIndexTest {

  def main(args: Array[String]): Unit = {
    val spark_conf = new SparkConf
    val spark = SparkSession.builder().master("local[*]").appName("test01").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    var rdd1 = sc.makeRDD(1 to 50)
    rdd1.mapPartitionsWithIndex(// 参数有两个 分区号 + 迭代器  =》 一个新的迭代器
      /*  统计每个分区的有多少元素*/
      (par_index, iter) => {
        val par_map = scala.collection.mutable.Map[String, Int]()  //准备一个map 存放 （分区号--》分区元素统计）
        while (iter.hasNext) {//开始遍历rdd分区里元素
          val par_name = "part_" + par_index//准备一个key
          if (par_map.contains(par_name)) {//如果原有集合里有key 那么在其value上 + 1
            val ele_cnt = par_map(par_name)
            par_map(par_name) = ele_cnt + 1
          } else {//没有的话 给一个初始值
            par_map(par_name) = 1
          }
          iter.next()//继续
        }
        par_map.iterator//返回一个迭代器
      }).collect().foreach(println(_))
    println("==================================================================================")
    rdd1.mapPartitionsWithIndex(
      (part_index,iter)=>{
        var map = scala.collection.mutable.Map[String,List[Int]]()
        while (iter.hasNext){
          val part_name= "part_"+part_index
          val elem = iter.next()
          if (map.contains(part_name)){
            var elems = map(part_name)
            elems ::= elem
            map(part_name)= elems
          }else{
          map(part_name)=List[Int]{elem}
          }
        }

        map.iterator
      }).collect().foreach(println(_))

  }


}
