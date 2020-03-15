package com.abiratsis.spark.streaming.extensions

import java.io.File

import com.abiratsis.spark.streaming.extensions.extensions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.io.Directory
import scala.util.{Failure, Success}

case class RowData(c1 : Int, c2 : Int, c3: String)

class FileSystemStopStreamingQueryTest extends FlatSpec {
   lazy val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

  import spark.implicits._
  private val tmpPath :String = "/tmp/stop_streaming/"

  val sampleDataPath = this.getClass
    .getResource("/stream_data.csv")
    .getPath()

  def cleanUpDir(path :String) : Unit = {
    val directory = new Directory(new File(path))
    directory.deleteRecursively()
    directory.createDirectory(true, false)
  }

  it should "should stop existing streaming job" in {
    cleanUpDir(tmpPath)

    val streamingQuery = spark
      .readStream
      .schema(Encoders.product[RowData].schema)
      .option("header", value = false)
      .option("delimiter", value = ";")
      .csv(new File(sampleDataPath).getParent)
      .as[RowData]

    val q = streamingQuery.writeStream
      .outputMode("append")
      .format("csv")
      .option("checkpointLocation", tmpPath)
      .option("path", tmpPath)
      .start()

    val stopStreamingDir = "/tmp/stop_streaming/"
    val stopStreamingPath = s"$stopStreamingDir/${q.id.toString}"

    // wait until stream processes some data
    while (q.isActive && q.recentProgress.length <= 0){
      Thread.sleep(100)
    }

    cleanUpDir(stopStreamingDir)

    val stopFile = new File(stopStreamingPath)

    // at the same time fire-up a thread which will wait a couple ms until awaitExternalTermination has been initialized
    // successfully and then remove the stop-file
    val removeStopFileFuture: Future[Boolean] = Future {
      while (!stopFile.exists()) {
        Thread.sleep(100)
      }
      stopFile.delete()

      true
    }

    removeStopFileFuture onComplete {
      case Success(_) => println(s"$stopStreamingPath was successfully removed.")
      case Failure(_) =>  fail(s"failed to remove: $stopStreamingPath.")
    }

    q.awaitExternalTermination(stopStreamingDir, q.id.toString, FileSystemType.LocalFileSystem)

    assert(q.isActive == false)
  }
}
