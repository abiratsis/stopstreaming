package eu.toon.streaming

import java.util.concurrent.ThreadLocalRandom

import eu.toon.streaming.fs._
import org.apache.spark.sql.streaming.StreamingQuery

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.{Failure, Success}

object extensions {

  object FileSystemType extends Enumeration {
    val DBFS, LocalFileSystem = Value
  }

  implicit class FileSystemStopStreamingQuery(val self :StreamingQuery) extends AnyVal {

    def awaitExternalTermination(streamStopDir :String, jobName :String, fsType : FileSystemType.Value): Unit ={

      if(streamStopDir == null || streamStopDir.isEmpty)
        throw new IllegalArgumentException("streamStopDir can't be null or empty.")

      if(jobName == null || jobName.isEmpty)
        throw new IllegalArgumentException("jobName can't be null or empty.")

      val fsWrapper :FileSystemWrapper = fsType match {
        case FileSystemType.DBFS => new DbfsWrapper(streamStopDir, jobName)
        case FileSystemType.LocalFileSystem => new LocalFileSystemWrapper(streamStopDir, jobName)
        case _ => throw new IllegalArgumentException("Invalid file system provided.")
      }

      val stopWatchFuture: Future[Boolean] = Future {

        if(!fsWrapper.targetFileExists)
            fsWrapper.createTargetFile(self.id.toString)

        while (self.isActive && fsWrapper.targetFileExists){
          val random: ThreadLocalRandom = ThreadLocalRandom.current()
          val r = random.nextLong(10, 100 + 1) // returns value between 10 and 100
          Thread.sleep(r)
        }

        if(!fsWrapper.targetFileExists){
          self.stop()
          true
        }
        else
          false
      }

      var output = "success"
      stopWatchFuture onComplete {
        case Success(result : Boolean) => if (!result) {
          output = s"failure: file not found."
        }
        case Failure(t) => output = s"failure: ${t.getMessage}."
      }

      self.awaitTermination()
    }
  }

}
