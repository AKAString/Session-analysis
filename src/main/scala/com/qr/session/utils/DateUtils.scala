package com.qr.session.utils

import java.text.SimpleDateFormat
import java.util.Date
import java.text.DecimalFormat


object DateUtils {
  private val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  private val DATE_FORMAT_YMDHMS = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
   private val DATE_FORMAT_YMDHH = new SimpleDateFormat("yyyy-MM-dd hh")
  private val NUMBER_FORMAT = new DecimalFormat("00")
  
  def numberFormat(v: Int) =
    {
      NUMBER_FORMAT.format(v)
    }
  def toYYYYMMDDHHMMSS(s:String)={
    DATE_FORMAT_YMDHMS.parse(s).getTime
  }
   def toYYYYMMDDHH(s:String)={
   DATE_FORMAT_YMDHH.format(DATE_FORMAT_YMDHMS.parse(s))
  }
  def main(args: Array[String]): Unit = {
   
    
    println(toYYYYMMDDHHMMSS("2018-12-21 03:59:25")- toYYYYMMDDHHMMSS("2018-12-21 03:02:40"))
  }
  
  
  def toDate() = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.format(new Date())
  }
  //2019-01-15 10:59:06
  def toSecondVal(dateStr:String)={
    val sdf=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
   
    sdf.parse(dateStr).getSeconds    
  }
}