package com.qr.session.analysis

import com.qr.session.utils.PropertyUtils
import com.qr.session.mock.DataMock
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import com.qr.session.useraction.sf.DaoFactory
import com.google.gson.GsonBuilder
import com.qr.session.task.TaskParam
import org.apache.spark.sql.DataFrame
import com.qr.session.utils.DateUtils
import com.qr.session.accumulator.SessionStepAccumulator
import scala.collection.mutable.Map
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Accumulable
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import java.util.Random
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD


object SessionAnalysis {
  def pageStep(pageCount: Int) = {
    var step = ""
    if (pageCount >= 0 && pageCount <= 3) {
      step = "1_3"
    }
    if (pageCount >= 4 && pageCount <= 6) {
      step = "4_6"
    }

    if (pageCount >= 7 && pageCount <= 9) {
      step = "7_9"
    }
    if (pageCount >= 10 && pageCount < 30) {
      step = "10_30"
    }
    if (pageCount >= 30 && pageCount < 60) {
      step = "30_60"
    }
    if (pageCount >= 60) {
      step = "60"
    }
    step

  }
  def secondToStep(sec: Int) = {
    var step = ""
    if (sec >= 0 && sec <= 3) {
      step = "1s_3s"
    }
    if (sec >= 4 && sec <= 6) {
      step = "4s_6s"
    }
    if (sec >= 7 && sec <= 9) {
      step = "7s_9s"
    }

    if (sec >= 10 && sec < 30) {
      step = "10s_30s"
    }

    if (sec >= 30 && sec < 60) {
      step = "30s_60s"
    }
    if (sec >= 60 && sec < 180) {
      step = "1m_3m"
    }
    if (sec >= 180 && sec < 600) {
      step = "3m_10m"
    }

    if (sec >= 600 && sec < 1800) {
      step = "10m_30m"
    }

    if (sec >= 1800) {
      step = "30m"
    }    
    step
  }
  //��������debug��mock��ȡ���ݣ�����Ǽ�Ⱥ���д�hiveȡ����
  def createSqlContext(sc: SparkContext) =
    {
      if (PropertyUtils.getProperty("isdebug").toBoolean) {
        val sqlContext = new SQLContext(sc)
        DataMock.mock(sqlContext, sc)
        sqlContext
      } else {
        new HiveContext(sc)
      }
    }

  def computeSessionSecondStepAndPageCount(sqlContext: SQLContext, rawDataFrame: DataFrame, stepAccBroadcast: Broadcast[Accumulable[Map[String, Int], Map[String, Int]]]) = {
    rawDataFrame.registerTempTable("sessioinStep")
    val actionTimeMaxMin = sqlContext.sql("select sessionid,max(actiontime),min(actiontime),count(distinct(pageid)) from sessioinStep group by sessionid")
    val actionTimeToSecondPageCount = actionTimeMaxMin.map { x =>
      {
        val secondStep = secondToStep(Math.abs(DateUtils.toSecondVal(x(1).toString()) - DateUtils.toSecondVal(x(2).toString())))
        stepAccBroadcast.value.add(Map[String, Int](secondStep -> 1))
        val pageCountStep = pageStep(x(3).toString().toInt)
        stepAccBroadcast.value.add(Map[String, Int](pageCountStep -> 1))
        stepAccBroadcast.value.add(Map[String, Int]("session_count" -> 1))
        (x(0).toString(), secondStep, pageCountStep)
      }
    }
    //actionTimeToSecondPageCount.foreach(x=>println(x+"+++====="))
    actionTimeToSecondPageCount.count()
    actionTimeMaxMin
  }
  def  secondPageCountDB(taskId:String,map: Map[String, Int])={ 
      DaoFactory.newSessionAggrStatDaoImp().addSessionAggrStat(taskId, map) 
     
  }
  def  sessionTop50DB(taskId:String,rdd:RDD[(String, (Int, (String, String)))]) ={ 
      DaoFactory.newSessionAggrStatDaoImp().addSessionRandomTop50(taskId, rdd) 
     
  }
  def sessionRandomExtract(actionTimeMaxMin:DataFrame,sessionCount:Int)={
    actionTimeMaxMin.registerTempTable("TimeMaxMin")
    val actionTimeMaxMinToymd=actionTimeMaxMin.map { x =>  
      val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val sdf1=new SimpleDateFormat("yyyy-MM-dd HH")
      val ymdh=sdf1.format(sdf.parse(x(1).toString()).getTime)
      (ymdh,x(0).toString(),x(1).toString(),x(2).toString(),x(3).toString())
    }
    val actionTimeMaxMinRdd=actionTimeMaxMinToymd.map{x=>(x._1,x._2)}.aggregateByKey(List[String]())((c:List[String],d:String)=>c.+:(d),(a:List[String],b:List[String])=>a++b)
 
    val sessionRandomTop50Rdd1=actionTimeMaxMinRdd.map{f=>
      val sessionIdRandom50=ListBuffer[String]()
      val random=new Random
      var c = (f._2.size.toFloat / sessionCount) * 50
      val fetchCount = Math.ceil(c).toInt
      var bool:Boolean=true
      while (bool) {
        val sessionId50=f._2(random.nextInt(f._2.size))
       if(!sessionIdRandom50.contains(sessionId50)){
      sessionIdRandom50.+=(sessionId50)
       }
       if(!(sessionIdRandom50.size<fetchCount)){
         bool=false
       } 
      }
      (f._1,sessionIdRandom50)
    }
  sessionRandomTop50Rdd1.flatMap(f=>f._2).map { x => (x,1) }.join(actionTimeMaxMinToymd.map { x => (x._2,(x._3,x._4))})
   
  
  }
  case class Category(categoryId: String, click: Int, order: Int, pay: Int)
  implicit def toCategory(cat: Category) = new Ordered[Category] {
    def compare(that: Category) = {
      if (cat.click == that.click) {
        if (cat.order == that.order) {
          that.pay - cat.pay
        } else {
          that.order - cat.order
        }
      } else {
        that.click - cat.click
      }
    }
  }
  def sessionTop10ByCategory(rawDataFrame:DataFrame)={
    
    val click=rawDataFrame.select("sessionid", "action","clickcategoryid").filter(rawDataFrame.col("action").equalTo("click")&&rawDataFrame.col("clickcategoryid")!=0).map{x=>(x(2).toString(),1)}.reduceByKey(_+_)
    val order=rawDataFrame.select("sessionid", "action","ordercategoryid").filter(rawDataFrame.col("action").equalTo("order")&&rawDataFrame.col("ordercategoryid")!=0).map{x=>(x(2).toString(),1)}.reduceByKey(_+_)
    val pay=rawDataFrame.select("sessionid", "action","paycategoryid").filter(rawDataFrame.col("action").equalTo("pay")&&rawDataFrame.col("paycategoryid")!=0).map{x=>(x(2).toString(),1)}.reduceByKey(_+_)
    val clickOrderPay=click.join(pay).join(order).map(x=>(Category(x._1,x._2._1._1,x._2._2,x._2._1._2))).takeOrdered(10)
   //println(clickOrderPay.toBuffer)
     val topClick = clickOrderPay.map { x => (x.categoryId, x.click) }
    
    (clickOrderPay, topClick)
  }
  def categoryTop10DB(taskId:String,categoryTop10: (Array[Category], Array[(String, Int)]))={
    DaoFactory.newSessionAggrStatDaoImp().addcategoryTop10(taskId, categoryTop10) 
  }
  def sessionBycategoryTop10(rawDataFrame:DataFrame,topClick10: RDD[(String, Int)])={
     val click=rawDataFrame.select("sessionid", "action","clickcategoryid").filter(rawDataFrame.col("action").equalTo("click")&&rawDataFrame.col("clickcategoryid")!=0).map{x=>(x(2).toString(),x(0).toString())}
     click.join(topClick10)
  }
  def sessionBycategoryTop10DB(taskId:String,topClick10Session:RDD[(String, (String, Int))])={
     DaoFactory.newSessionAggrStatDaoImp().addsessionBycategoryTop10(taskId, topClick10Session)
  }
  def main(args: Array[String]): Unit = {
     Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    if (args.length > 0) {
      val taskId = args(0)
      val conf = new SparkConf()
      conf.setAppName("sessionAnalysis")
      conf.setMaster("local")
      val sc = new SparkContext(conf)
      val sqlContext = createSqlContext(sc)
      val param = DaoFactory.newTaskDaoImp().getTaskParam(taskId.toInt)
   
      val gson = new GsonBuilder().create()
      val taskParam = gson.fromJson(param, classOf[TaskParam])
      //������ɸѡsession
      val rawDataFrame = sqlContext.sql("select uv.* from userVisit uv,users u where uv.userId=u.userId and userAge >=" + taskParam.minAge + " and userAge <=" + taskParam.maxAge)
 
      //=====ͳ�Ƴ�����������session�У�����ʱ����1s~3s��4s~6s��7s~9s��10s~30s��30s~60s��1m~3m��3m~10m��10m~30m��30m���ϸ�����Χ�ڵ�sessionռ�ȣ����ʲ�����1~3��4~6��7~9��10~30��30~60��60 ��ʼ=====
      val stepAcc = sc.accumulable( Map[String,Int]("session_count"->0,"1s_3s"->0,"4s_6s"->0,"7s_9s"->0,"10s_30s"->0,"1m_3m"->0,"3m_10m"->0,"10m_30m"->0,"30m"->0,"30s_60s"->0,"1_3"->0,"4_6"->0,"7_9"->0,"10_30"->0,"30_60"->0,"60"->0))(new SessionStepAccumulator())
      val stepAccBroadcast = sc.broadcast(stepAcc) 
       //secondPageCountDB(taskId,stepAccBroadcast.value.value)
      //           yyyy-MM-dd hh:mm:ss->yyyy-MM-dd hh
      //(sessionid,maxactiontime,minactiontime,pagecount)
      val actionTimeMaxMinPageCount=computeSessionSecondStepAndPageCount(sqlContext, rawDataFrame, stepAccBroadcast)
      //actionTimeMaxMinPage ount.show()
      
      val sessiontop50=sessionRandomExtract(actionTimeMaxMinPageCount,stepAccBroadcast.value.value.getOrElse("session_count", 1))
      sessionTop50DB(taskId,sessiontop50)
      
       val categoryTop10=sessionTop10ByCategory(rawDataFrame)
       //categoryTop10DB(taskId,categoryTop10)
       
       
       val topClick10Rdd=sc.parallelize(categoryTop10._2)
       val sessionClickTop10=sessionBycategoryTop10(rawDataFrame,topClick10Rdd)
       //sessionBycategoryTop10DB(taskId,sessionClickTop10)
       
      //=====ͳ�Ƴ�����������session�У�����ʱ����1s~3s��4s~6s��7s~9s��10s~30s��30s~60s��1m~3m��3m~10m��10m~30m��30m���ϸ�����Χ�ڵ�sessionռ�ȣ����ʲ�����1~3��4~6��7~9��10~30��30~60��60 ����=====
      //=====�ڷ���������session�У�����ʱ����������ȡ50��session start==========================
            //1.��actionTimeMaxMinPageCount����ת��(yyyy-MM-dd hh,sessionId)
            //2.aggregteByKey()[yyyy-MM-dd hh->[seessionid,sessionid,......]]  (����Ԫ�ظ���.toFloat/50.toFlloat)*50
            //3.yyyy-MM-dd hh->[seessionid,sessionid,......]===>rdd[(sessionid,yyyy-MM-dd hh)]
            //4.join(actionTimeMaxMinPageCount.join(rdd))
            //5.������ݿ�
      //=====�ڷ���������session�У�����ʱ����������ȡ50��session end============================
      //======�ڷ���������session�У���ȡ������µ���֧����������ǰ10��Ʒ�� start=========================
         //1.rawDataFrame ����=>click
         //val clickcategoryidDF= rawDataFrame.select("clickcategoryid").where("action='click'")
              // ==(clickcategoryid,1)=>reduceByKey
         //2.rawDataFrame ����=>pay
         //val paycategoryidDF= rawDataFrame.select("paycategoryid").where("action='pay'")
          // ==(paycategoryid,1)=>reduceByKey==
         //3rawDataFrame ����=>order
         //val ordercategoryidDF= rawDataFrame.select("ordercategoryid").where("action='order'")
         //==(ordercategoryid,1)=>reduceByKey
         //join.join==>(1,3,4,5)=>����
      //======�ڷ���������session�У���ȡ������µ���֧����������ǰ10��Ʒ�� end============================
      //===��������ǰ10��Ʒ�࣬�ֱ��ȡ������������ǰ10��session start===================
      //clickcategoryidDF(clickcategoryid,14) ,(clickcategoryid,133) ,(clickcategoryid,443) 
      //join  rawDataFrame
      //(sessionid,2)  join rawDataFrame(sessionid,row)
      //==��������ǰ10��Ʒ�࣬�ֱ��ȡ������������ǰ10��session  end====================

    } else {
      println("��������ȷ")
    }

  }
}