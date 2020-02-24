package cn.itcast.sql

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 创建样例类
  *
  * @param id
  * @param name
  * @param age
  */
case class Person(id: Int, name: String, age: Int)

/**
  * 向MySQL数据库写入数据
  */
object SparkSqlToMysql {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().
      appName("SparkSqlToMysql").
      master("local[2]")
      .getOrCreate()
    // 2.创建数据（两个person数据）
    val data: RDD[String] = spark.sparkContext
      .parallelize(Array("3,wangwu,22", "4,zhaoliu,26"))
    // 3.按MySQL列名切分数据
    val arrRDD: RDD[Array[String]] = data.map(_.split(","))
    // 4.RDD关联（匹配）Person样例类
    val personRDD: RDD[Person] = arrRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))
    // 导入隐式转换
    import spark.implicits._
    // 5.将RDD转换成DataFrame
    val personDF: DataFrame = personRDD.toDF()
    // 6.设置JDBC配置参数，用于访问MySQL数据库
    val prop: Properties = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    // 7.写入数据
    personDF.write.mode("append").jdbc("jdbc:mysql://node-1:3306", "spark.person", prop)
    personDF.show()
  }

}
