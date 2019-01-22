package com.qr.session.dao

import com.qr.spark.useraction.jdbc.JDBCManager

trait TaskDao {
  def getTaskParam(taskId: Int) = {
    val rs = new JDBCManager().executeQuery("select task_param from task where task_id=" + taskId)
    var result = ""
    if (rs.next()) {
      result = rs.getString(1)
    }
    result
  }
}