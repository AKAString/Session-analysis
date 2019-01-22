package com.qr.session.useraction.sf

import com.qr.session.dao.TaskDaoImp
import com.qr.session.dao.SessionAggrStatDaoImp
import com.qr.session.dao.Session_random_extractDaoImp


object DaoFactory {
  def newTaskDaoImp()=new TaskDaoImp()
  def newSessionAggrStatDaoImp()=new SessionAggrStatDaoImp()
  def newSessionRanom_extractDaoImp()=new Session_random_extractDaoImp()
  
}