package cn.itcast.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 编程方式定义Schema
  */
object SparkSqlSchema {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("SparkSqlSchema").master("local[2]").getOrCreate()
    // 2.获取SparkContext对象
    val sc: SparkContext = spark.sparkContext
    // 设置日志打印级别
    sc.setLogLevel("WARN")
    // 3.加载数据
    val dataRDD: RDD[String] = sc.textFile("D://spark//person.txt")
    // 4.切分每一行
    val dataArrayRDD: RDD[Array[String]] = dataRDD.map(_.split(" "))
    // 5.加载数据到ROW对象中
    val personRDD: RDD[Row] = dataArrayRDD.map(x => Row(x(0).toInt, x(1), x(2).toInt))
    // 6.创建Schema
    val schema: StructType = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("age", IntegerType, false)
    ))
    // 7.利用personRDD与Schema创建DataFrame
    val personDF: DataFrame = spark.createDataFrame(personRDD, schema)
    // 8.DSL操作显示DataFrame的数据结果
    personDF.show()
    // 9.将DataFrame注册成表
    personDF.createOrReplaceTempView("t_person")
    // 10.sql语句
    spark.sql("select * from t_person").show()
    // 11.关闭资源
    sc.stop()
    spark.stop()
  }

}
