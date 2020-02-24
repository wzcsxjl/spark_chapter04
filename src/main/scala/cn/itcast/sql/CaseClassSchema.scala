package cn.itcast.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 定义样例类
  */
case class Person(id: Int, name: String, age: Int)

/**
  * 反射机制推断Schema
  */
object CaseClassSchema {

  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("CaseClassSchema").master("local[2]").getOrCreate()
    // 2.获取SparkContext
    val sc: SparkContext = spark.sparkContext
    // 设置日志打印级别
    sc.setLogLevel("WARN")
    // 3.读取文件
    val data: RDD[Array[String]] = sc.textFile("D://spark//person.txt").map(x => x.split(" "))
    // 4.将RDD与样例类关联
    val personRdd: RDD[Person] = data.map(x => Person(x(0).toInt, x(1), x(2).toInt))
    // 5.获取DataFrame
    // 手动导入隐式转换
    import spark.implicits._
    val personDF: DataFrame = personRdd.toDF()
    // ------------DSL语法操作开始-------------
    // 1.显示DataFrame的数据，默认显示20行
    personDF.show()
    // 2.显示DataFrame的schema信息
    personDF.printSchema()
    // 3.统计DataFrame中年龄大于30岁的人数
    println(personDF.filter($"age" > 30).count())
    // -----------DSL语法操作结束-------------
    // -----------SQL操作风格开始-------------
    // 将DataFrame注册成表
    personDF.createOrReplaceTempView("t_person")
    spark.sql("select * from t_person").show()
    spark.sql("select * from t_person where name = 'zhangsan'").show()
    // -----------SQL操作风格结束-------------
    // 关闭资源操作
    sc.stop()
    spark.stop()
  }

}
