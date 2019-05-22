package Utility_TaskLoader

import java.net.URI
import java.sql.{Connection, DriverManager, Timestamp}
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ContentSummary, FileSystem, Path}
import Utils.Context

object taskLoader extends Context
{
  def main(args: Array[String]): Unit = {

    val url = "jdbc:mysql://172.23.87.139:3306/staging"
    val driver = "org.mariadb.jdbc.Driver"
    val username = "bi_revamp"
    val password = "cloudera"
    var connection: Connection = null
    val conf = new Configuration()
    val fs = FileSystem.get(new URI("hdfs://KEKEDTEVDBDT19:8020"), conf)
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      val rs = statement.executeUpdate("TRUNCATE TABLE TaskQueue_Stage")
      val rs1 = statement.executeQuery("SELECT SourceName, FileSpec, SourcePath,DirectoryID FROM TaskQueueDirectories where ActiveFlag = 1")
      while (rs1.next) {
        var SourceName = rs1.getString("SourceName")
        var SourcePath = rs1.getString("SourcePath")
//        var ArchivePath = rs1.getString("ArchivePath")
        var FileSpec = rs1.getString("FileSpec")
        var DirectoryID = rs1.getInt("DirectoryID")
        var start = FileSpec.split("\\*")(0)
        var end = FileSpec.split("\\*")(1)
        val status = fs.listStatus(new Path(SourcePath))
        for(child <- status.par)
        {
          val file_path = child.getPath

          if (file_path.getName.startsWith(start) && file_path.getName.endsWith(end)) {

            var FileName = file_path.getName
//            var Archive_Path = ArchivePath
            var FilePath = file_path.toString
            var Status = 0
            val contentSummary : ContentSummary = fs.getContentSummary(file_path)
            var Size : Long = contentSummary.getLength
            var DateStarted = null
            var DateCompleted = null
            var cal = Calendar.getInstance()
            val DateEntered = new Timestamp(cal.getTimeInMillis)
            var statement2 = connection.createStatement
            var rs2 = statement2.executeUpdate("Call InsertTaskQueue_Stage ('" + SourceName + "','" + SourcePath + "','" + FileName + "','" + Status + "','" + Size + "','" + DirectoryID + "','" + DateEntered + "','" + FileSpec + "')")
            statement2.close()
          }
        }
      }
      statement.close()
      val statement3 = connection.createStatement
      val rs3 = statement3.executeUpdate("INSERT INTO TaskQueue (SourceName, FilePath, FileName, Status, Size, PPN_DT, DateStarted, DirectoryID,DateEntered) SELECT tqs.SourceName, tqs.FilePath, tqs.FileName, tqs.Status, tqs.Size, tqs.PPN_DT, tqs.DateStarted, tqs.DirectoryID,tqs.DateEntered FROM TaskQueue_Stage tqs LEFT JOIN TaskQueue tq ON tqs.SourceName = tq.SourceName AND tqs.PPN_DT = tq.PPN_DT AND tqs.FileName = tq.FileName Where tq.FileName IS NULL AND tqs.Size > 0")
      val rs4 = statement3.executeUpdate("INSERT INTO TaskQueue (SourceName, FilePath, FileName, Status, Size, PPN_DT, DateStarted, DirectoryID,DateEntered) SELECT tqs.SourceName, tqs.FilePath, tqs.FileName, 3, tqs.Size, tqs.PPN_DT, tqs.DateStarted, tqs.DirectoryID,tqs.DateEntered FROM TaskQueue_Stage tqs LEFT JOIN TaskQueue tq ON tqs.SourceName = tq.SourceName AND tqs.PPN_DT = tq.PPN_DT AND tqs.FileName = tq.FileName Where tq.FileName IS NULL AND tqs.Size = 0")
      statement3.close()
      val statement4 = connection.createStatement
      val rs5 = statement4.executeUpdate("INSERT INTO TaskQueue (SourceName, FilePath, FileName, Status, Size, PPN_DT, DateStarted, DirectoryID,DateEntered) SELECT tqs.SourceName, tqs.FilePath, tqs.FileName, 4, tqs.Size, tqs.PPN_DT, tqs.DateStarted, tqs.DirectoryID, tqs.DateEntered FROM TaskQueue_Stage tqs INNER JOIN TaskQueue tq ON tqs.SourceName = tq.SourceName AND tqs.PPN_DT = tq.PPN_DT AND tqs.FileName = tq.FileName WHERE tq.Status = 2")
      statement4.close()
//    val statement6 = connection.createStatement
//      val rs6 = statement6.executeQuery("Select * from TaskQueue where Status = 3")
//      while(rs6.next)
//        {
////          var status_check = rs6.getInt("Status")
//          var filepath = rs6.getString("FilePath")
//          var archivePath = rs6.getString("ArchivePath")
//          var filename = rs6.getString("FileName")
//          fs.rename(new Path(filepath + filename), new Path(archivePath + filename))
//        }
//      statement6.close()
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    connection.close()
  }
}