import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}


object testDate {
  def main(args: Array[String]): Unit = {
    //    println(new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
    //   val s = DateUtils.addDays(new Date(),1)
    //    println(s)
    //    val d = new SimpleDateFormat("yyyy-MM-dd").format(s)
    //    println(d)
    //    val dd= DateUtils.addDays(new Date(),-1)
    //   println( DateFormatUtils.format(dd,"yyyy-MM-dd"))

    //获取当天或者昨天或者前几天日期并且按照预定格式输出！
    val yesterday = DateFormatUtils.format(DateUtils.addDays(new Date(), -2), "yyyy-MM-dd")
    println(yesterday)
    //StringUtils的使用 isEmpty,isNotEmpty
    println("========================isEmpty=========================")
    val r = StringUtils.isEmpty(" ") //false
    val r0 = StringUtils.isEmpty("") //true
   println("null"+StringUtils.isEmpty(null)) //true
    println("r=" + r) //true
    println("r0=" + r0) //true
    val r1 = StringUtils.isNotEmpty("zhaojun") //true
    println("r1=" + r1)
    val r2 = StringUtils.isNotEmpty(null) //false
    println("r2=" + r2) //false
    println("========================defaultIfEmpty=========================")
    println(StringUtils.defaultIfEmpty(null, "NULL")) //Null
    println(StringUtils.defaultIfEmpty("", "NULL")) //Null
    println("空白" + StringUtils.defaultIfEmpty(" ", "NULL")) //  !!" "这个不为空
    println(StringUtils.defaultIfEmpty("bat", "NULL")) //bat
    println(StringUtils.defaultIfEmpty("", null)) // null

    println("=====================isBlank============================")
    println(StringUtils.isBlank(""))
    println(StringUtils.isBlank(null))
    println(StringUtils.isBlank(""))
    println(StringUtils.isBlank(" "))
    println(StringUtils.isBlank("bob"))
    println(StringUtils.isBlank("  bob  "))

    /**
      * 总结
      * 1、对于 "" isblank 和 isEmpty 返回的是true
      * 2、对于 "  "isblank返回的是true,isEmpty返回的是false
      * 3、对于 null  isblank 和 isEmpty 返回的是true
      */
  }

}
