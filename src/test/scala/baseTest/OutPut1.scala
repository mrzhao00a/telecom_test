package baseTest

object OutPut1 {
  def main(args: Array[String]): Unit = {
    val name ="zhaojun"
    val age = 30
    val heigh = 170.0
    val slaly =130111100.12
    printf("name=%s age=%s heigh=%.2f slaly%.3f",name,age,heigh,slaly)  //%s String， %d int % f double
    println(s"个人信息\n name:$name \nage:$age \nheigh:$heigh \nslaly:$slaly")
    println(s"个人信息\n name:${name+ "liumiaomiao"} \nage:${age+1} \nheigh:${heigh+1} \nslaly:${slaly-1000}")//sql语句中引用表名字段
  }

}
