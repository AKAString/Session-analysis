package com.qr.session.dao

import com.qr.spark.useraction.jdbc.JDBCManager
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD
import com.qr.session.analysis.SessionAnalysis.Category




trait SessionAggrStatDao {
  def addSessionAggrStat(taskId: String, map: Map[String, Int]) = {
    val sql = "insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    val paramsList = new ListBuffer[ArrayBuffer[Any]]()
    val param = new ArrayBuffer[Any]()
    param.+=(taskId)
    param.+=(map("session_count"))
    param.+=(map("1s_3s"))
    param.+=(map("4s_6s"))
    param.+=(map("7s_9s"))
    param.+=(map("10s_30s"))
    param.+=(map("30s_60s"))
    param.+=(map("1m_3m"))
    param.+=(map("3m_10m"))
    param.+=(map("10m_30m"))
    param.+=(map("30m"))
    param.+=(map("1_3"))
    param.+=(map("4_6"))
    param.+=(map("7_9"))
    param.+=(map("10_30"))
    param.+=(map("30_60"))
    param.+=(map("60"))
    paramsList.+=(param)
    new JDBCManager().executeBatch(sql, paramsList);
  }
  def addSessionRandomTop50(taskId: String, rdd:RDD[(String,(Int,(String,String)))]) = {

    rdd.foreachPartition(f=>{
      while(f.hasNext){
        var r=f.next()
        val sql="insert into session_random_extract valuers(?,?,?,?)"
        val paramsList = new ListBuffer[ArrayBuffer[Any]]()
        val param = new ArrayBuffer[Any]()
          param.+=(taskId)
          param.+=(r._1)
          param.+=(r._2._2._2)
          param.+=(r._2._2._1)
          paramsList.+=(param)
          new JDBCManager().executeBatch(sql, paramsList);
      }
    })
  }
  def addcategoryTop10(taskId: String, categoryTop10:(Array[Category],Array[(String,Int)])) = {
    val sql="insert into top10_category values(?,?,?,?,?)"
    categoryTop10._1.foreach { x =>  
    val paramsList = new ListBuffer[ArrayBuffer[Any]]()
    val param = new ArrayBuffer[Any]()
    param.+=(taskId)
    param.+=(x.categoryId)
    param.+=(x.click)
    param.+=(x.order)
    param.+=(x.pay)
    paramsList.+=(param)
    new JDBCManager().executeBatch(sql, paramsList);
    }
    //categoryId:String,click:Int,order:Int,pay:Int
  }
  
  def addsessionBycategoryTop10(taskId:String,topClick10Session:RDD[(String,(String,Int))]){
    val sql="insert into top10_category_session values(?,?,?,?)"
    topClick10Session.foreach{f=>
      val paramsList = new ListBuffer[ArrayBuffer[Any]]()
      val param = new ArrayBuffer[Any]()
      param.+=(taskId)
      param.+=(f._1)
      param.+=(f._2._1)
      param.+=(f._2._2)
      paramsList.+=(param)
       new JDBCManager().executeBatch(sql, paramsList);
    }
  }
}

















