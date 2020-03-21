# Stop streaming gracefully

The purpose of this project is to be able to stop a streaming job through the file system, at the moment Databricks DBFS and local FS are supported.

The need arises from the fact that it is too hard to access Spark context from a notebook in order to call `stop` method of the `StreamingQuery` which is responsible for stopping the query gracefully.

The solution is based on a file watcher (Scala future) which runs asynchronously and it keeps the streaming job running as long as the corresponding file exists in the predefined directory. When the file is deleted the `stop` method is called stopping streaming gracefully. This allows us to control the lifetime of the streaming job via a shared directory without accessing the Spark context. 

The implementation extends the functionality of the build-in Spark class `StreamingQuery` with the method `awaitExternalTermination(streamStopDir, jobName, fsType)`. 

## Usage

Write your streaming program, then call `awaitExternalTermination` instead of `awaitTermination` with the following arguments:

- streamStopDir: the directory to be watched
- jobName: the name of the current job. This is used to name the file that will be saved at `streamStopDir`
- fsType: one of the `[DBFS, LocalFileSystem]` 

To stop the job execute `%fs rm -r your_path` from your Databricks notebook or CLI.

## Scala example

```scala
val streamingQuery = spark
      .readStream
      .csv("some_path")

val sq = streamingQuery.writeStream
      .outputMode("append")
      .format("csv")
      .option("path", "some_path")
      .start()

val stopStreamingDir = "some_dbfs_path"

.........

sq.awaitExternalTermination(stopStreamingDir, sq.id.toString, FileSystemType.DBFS)
```