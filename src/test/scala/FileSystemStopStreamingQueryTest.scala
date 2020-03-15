package eu.toon.streaming

import eu.toon.storage.schema.RawCsvSchema
import eu.toon.test.SharedSparkSession.spark
import org.apache.spark.sql.Encoders
import org.scalatest.FlatSpec
import java.io.File

import scala.reflect.io.Directory
import extensions._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class FileSystemStopStreamingQueryTest extends FlatSpec {
  import spark.implicits._

  private val tmpPath :String = "/tmp/eneco.streaming.toon.eu/csv_export_2019_12_11/"

  val sampleDataPath = this.getClass
    .getResource("/stop_streaming/ACCEP-2019-12-11T000521.289Z-a6b29bb0-cc78-4f3c-a2e5-cc5f0c4e1af3.completed.csv")
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
      .schema(Encoders.product[RawCsvSchema].schema)
      .option("header", value = false)
      .option("delimiter", value = ";")
      .csv(new File(sampleDataPath).getParent)
      .as[RawCsvSchema]

    val q = streamingQuery.writeStream
      .outputMode("append")
      .format("csv")
      .option("checkpointLocation", tmpPath)
      .option("path", tmpPath)
      .start()

    val stopStreamingDir = "/tmp/eneco.streaming.toon.eu/stop_streaming/"
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
