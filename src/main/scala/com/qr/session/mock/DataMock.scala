package com.qr.session.mock

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import com.qr.session.utils.DateUtils
import scala.util.Random
import java.util.UUID
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.SparkConf
import java.text.DecimalFormat
object DataMock {
  //date userid sessionid,actiontime,action,paycategoryid,payproductid,clickcategoryid,
  //clickproductid,ordercategoryid,orderproductid,searchkeys,pageid
  def decimalFormat(num:Int)={
    val dec=new DecimalFormat("00")
    dec.format(num)
  }
  def mock(sqlContext: SQLContext, sc: SparkContext) = {
    val date = DateUtils.toDate()
    //time yyyy-MM-dd hh:mm:ss
    val actions = Array("click", "pay", "order", "searchkeys")
    val random = new Random()
    val searchKeys = Array("火锅", "蛋糕", "重庆辣子鸡", "重庆小面", "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
    var listBuffer = new ListBuffer[Row]();
    //生成一100个用户
    for (i <- 0.until(100)) {

      var userId = random.nextInt(100)
      var sessionId = ""
      var actiontime = ""
      var action = ""
      var clickcategoryid = 0
      var clickproductid = 0
      var paycategoryid = 0
      var payproductid = 0
      var ordercategoryid = 0
      var orderproductid = 0
      var searchKey = ""
      val baseDate = date + " " + decimalFormat(random.nextInt(23))
      for (j <- 0.until(10)) {
        sessionId = UUID.randomUUID().toString().replace("-", "")
        for (a <- 0.until(random.nextInt(100))) {
          actiontime = baseDate + ":" + decimalFormat(random.nextInt(60)) + ":" + decimalFormat(random.nextInt(60))
          action = actions(random.nextInt(actions.length))
          if (action.equals("click")) {
            clickcategoryid = random.nextInt(100)
            clickproductid = random.nextInt(100)
            paycategoryid = 0
            payproductid = 0
            ordercategoryid = 0
            orderproductid = 0
            searchKey = ""
          }
          if (action.equals("pay")) {
            paycategoryid = random.nextInt(100)
            payproductid = random.nextInt(100)
            ordercategoryid = 0
            orderproductid = 0
            clickcategoryid = 0
            clickproductid = 0
            searchKey = ""
          }
          if (action.equals("order")) {
            ordercategoryid = random.nextInt(100)
            orderproductid = random.nextInt(100)
            clickcategoryid = 0
            clickproductid = 0
            paycategoryid = 0
            payproductid = 0
            searchKey = ""
          }
          if (action.equals("searchkeys")) {
            searchKey = searchKeys(random.nextInt(searchKeys.length))
            clickcategoryid = 0
            clickproductid = 0
            paycategoryid = 0
            payproductid = 0
            ordercategoryid = 0
            orderproductid=0
          }
          var pageid=random.nextInt(100)
          //
          listBuffer.+=:(Row(date, userId, sessionId, actiontime, action, paycategoryid, payproductid, clickcategoryid, clickproductid, ordercategoryid, orderproductid, searchKey,pageid))
        }
      }

    }
    val rddUserAction = sc.parallelize(listBuffer)
    val schema = StructType(Array(StructField("date", StringType, true), StructField("userId", IntegerType, true),
      StructField("sessionid", StringType, true), StructField("actiontime", StringType, true), StructField("action", StringType, true), StructField("paycategoryid", IntegerType, true), StructField("payproductid", IntegerType, true), StructField("clickcategoryid", IntegerType, true), StructField("clickproductid", IntegerType, true), StructField("ordercategoryid", IntegerType, true), StructField("orderproductid", IntegerType, true), StructField("searchkey", StringType, true), StructField("pageid", IntegerType, true)))
    val df = sqlContext.createDataFrame(rddUserAction, schema)
    df.registerTempTable("userVisit")
    var rowsBuffer = new ListBuffer[Row]();
    for (i <- 0.until(100)) {

      val userId = i;
      
      val userName = "name" + i;
      val userSex = "sex" + i;
      val userCity = i
      val professional = "professional" + i;
      val userAge = random.nextInt(60)
      rowsBuffer.+=:(Row(userId, userName, userSex, userCity, professional, userAge))
    }
    val userSchema = StructType(Array(StructField("userId", IntegerType, true), StructField("userName", StringType, true), StructField("userSex", StringType, true), StructField("userCity", IntegerType, true),  StructField("professional", StringType, true),StructField("userAge", IntegerType, true)))
    val userRDD = sc.parallelize(rowsBuffer)
    val userDF = sqlContext.createDataFrame(userRDD, userSchema)
    userDF.registerTempTable("users")
    sqlContext
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("userAction")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    mock(sqlContext, sc)
    sqlContext.sql("select * from userVisit").show()

  }
}