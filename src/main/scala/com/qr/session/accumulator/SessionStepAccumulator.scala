package com.qr.session.accumulator

import org.apache.spark.AccumulatorParam
import scala.collection.mutable.Map
/**
 * 累加器
 */
class SessionStepAccumulator extends AccumulatorParam[Map[String, Int]] {
  def addInPlace(r1: Map[String, Int], r2: Map[String, Int]) = {
    if (r1 == null) {
      r2
    } else {
      val key = r2.keySet.iterator.next()
        val value = r2.getOrElse(key, 0)
        r1.+=((key, r1.getOrElse(key, 0) + value))
      r1
    }
  }  
  def zero(initMap: Map[String, Int]): Map[String, Int] = {
    Map[String, Int]("session_count" -> 0, "1s_3s" -> 0, "4s_6s" -> 0, "7s_9s" -> 0, "10s_30s" -> 0, "1m_3m" -> 0, "3m_10m" -> 0, "10m_30m" -> 0, "30m" -> 0, "30s_60s" -> 0, "1_3" -> 0, "4_6" -> 0, "7_9" -> 0, "10_30" -> 0, "30_60" -> 0, "60" -> 0)
 
  }
}