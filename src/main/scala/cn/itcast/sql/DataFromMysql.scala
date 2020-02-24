package cn.itcast.sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 读取MySQL数据库
  */
object DataFromMysql {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName("DataFromMysql").master("local[2]").getOrCreate()
    // 2.创建Properties对象，设置连接MySQL的用户名和密码
    val properties: Properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    // 3.读取MySQL中的数据
    val mysqlDF: DataFrame = spark.read.jdbc("jdbc:mysql://node-1:3306/spark", "person", properties)
    // 4.显示MySQL中表的数据
    mysqlDF.show()
    spark.stop()
  }

}
