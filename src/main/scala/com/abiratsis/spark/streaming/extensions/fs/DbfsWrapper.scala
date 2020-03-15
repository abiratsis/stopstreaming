package eu.toon.streaming.fs

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

class DbfsWrapper(val stopDir: String, val targetFile: String) extends FileSystemWrapper {
  override def targetFileExists(): Boolean = {
    try {
      dbutils.fs.ls(targetPath).size > 0
    }
    catch {
      case _: java.io.FileNotFoundException => false
    }
  }

  override def createTargetFile(content: String): Unit = {
    dbutils.fs.put(targetPath, content)
  }
}
