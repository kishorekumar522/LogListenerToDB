package com.actor

import akka.actor.ActorSystem
import com.typesafe.config.Config
import akka.actor.Actor
import akka.actor.ActorLogging
import java.sql.Connection
import com.typesafe.config.ConfigFactory
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.util.regex.Pattern
import java.util.regex.Matcher
import org.apache.commons.io.input.TailerListenerAdapter
import org.apache.commons.io.input.Tailer
import scala.concurrent._
import akka.actor.Props
import akka.actor.ActorRef

/**
 * @author Kishore_Yakkala
 */
object LogMonitToDB {
  
  def main(args:Array[String])={
      
    var appConf = ConfigFactory.load()
    println(appConf.getString("logprocessor.name") + " app Started")
    var system = ActorSystem("gosslogging")
    import scala.concurrent.ExecutionContext.Implicits.global

    val monitFilePaths = appConf.getList("logprocessor.log.watchfile").unwrapped().iterator()
    val originator = appConf.getString("logprocessor.log.originator")
    val dbLogger = system.actorOf(Props(new LogProcessor(appConf, originator)), name = "DBLogging")
    val poolInterval = appConf.getString("logprocessor.log.pollinterval").toInt
    var logListener : LogListener = null
    
    while(monitFilePaths.hasNext()){
      val monitFilePath = monitFilePaths.next().toString
      future{
    	  println("Initiating Logger for File : " + monitFilePath)
       logListener = new LogListener(dbLogger)
       new Tailer(new File(monitFilePath), logListener, poolInterval).run
       
      }
    }
  }
}

case class LogEvent(l_originator_system:Some[String], l_goss_event:Some[String], l_order_number:Some[String], l_bu_id:Some[String], l_header_id:Some[String], l_msg_event:Some[String], l_msg_process_thread:Some[String], l_log_thread:Some[String], l_log_date:Some[String], l_time_taken:Some[String], l_log_level:Some[String])

class LogProcessor(conf:Config, originator:String) extends Actor with ActorLogging{
  
  var insertSql:String = "insert into " + conf.getString("logprocessor.db.tablename") + " (originator_system, goss_event, order_number, bu_id, header_id, msg_event, msg_process_thread, log_thread, log_date, time_taken, log_level) values (?,?,?,?,?,?,?,?, to_timestamp(?,'DD MON YYYY HH24:MI:SS.FF3'),?,?)"
  var dbCon:Connection = null
  var stmt:PreparedStatement = null

  var logEntryPattern = "([\\d]+\\s[\\w]+\\s[\\d]+\\s[\\d:,]+)\\s([\\w]+)\\s+\\[(.*)\\]\\s+\\((.*)\\)\\s+-\\s\\{GOSSEvent=(.*);headerid=(.*);ordernumber=(.*);buid=(.*);eventstatus=(.*);timetaken=(.*)\\}"
  var pattern = Pattern.compile(logEntryPattern)
  var matcher : Matcher = null

  override def preStart()={
    Class.forName("oracle.jdbc.driver.OracleDriver")
    dbCon = DriverManager.getConnection(conf.getString("logprocessor.db.url"), conf.getString("logprocessor.db.username"),conf.getString("logprocessor.db.password"))
    println("Init DB connection")
  }
  
  def receive={
    case ldata:String =>{
      insertToDB(shardRawLog(ldata))
    }
    case x => log.error("Invalid Input : " + x)
  }
  
  def insertToDB(logArr : Array[(LogEvent)])={
      stmt = dbCon.prepareStatement(insertSql)
      for(event<-logArr){
        stmt.setString(1, event.l_originator_system.getOrElse(""))
        stmt.setString(2, event.l_goss_event.getOrElse(""))
        stmt.setString(3, event.l_order_number.getOrElse(""))
        stmt.setString(4, event.l_bu_id.getOrElse(""))
        stmt.setString(5, event.l_header_id.getOrElse(""))
        stmt.setString(6, event.l_msg_event.getOrElse(""))
        stmt.setString(7, event.l_msg_process_thread.getOrElse(""))
        stmt.setString(8, event.l_log_thread.getOrElse(""))
        stmt.setString(9, event.l_log_date.getOrElse(""))
        stmt.setString(10, event.l_time_taken.getOrElse(""))
        stmt.setString(11, event.l_log_level.getOrElse(""))
        
        stmt.addBatch()
      }
      stmt.executeBatch()
      stmt.close()  

  }

  def shardRawLog(logEntryLines:String):Array[LogEvent]={
      
    var event:LogEvent = null
    
    logEntryLines.split(System.lineSeparator()).map(line =>{
      
      matcher = pattern.matcher(line)
      if(matcher.matches){
        event = LogEvent(Some(originator), Some(matcher.group(5)),Some(matcher.group(7)),Some(matcher.group(8)),Some(matcher.group(6)),
            Some(matcher.group(9)),Some(matcher.group(3)),Some(matcher.group(4)),Some(matcher.group(1).replace(",", ".")),Some(matcher.group(10)),Some(matcher.group(2)))
      }
      event
    })
  }

  override def postStop()={
    dbCon.commit()
    dbCon.close()
  }
  
  override def preRestart(reason:Throwable, message:Option[Any])={
    println("Prestarting the job, due to : " + message)
    preStart()
  }
}

class LogListener(dbLogger:ActorRef) extends TailerListenerAdapter{

  override def handle(lines:String)={
    dbLogger ! lines
  }
}
