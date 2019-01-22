package com.qr.session.utils

import java.util.Properties
import java.io.FileInputStream

object PropertyUtils {
  val props = new Properties()
  props.load(PropertyUtils.getClass.getResourceAsStream("/app.properties"))
  def getProperty(key: String) = {
    props.getProperty(key)
  }
  def main(args: Array[String]): Unit = {
   println(props.getProperty("jdbc.url"))
  }
}