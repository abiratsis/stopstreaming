package eu.toon.streaming.fs

class LocalFileSystemWrapper(val stopDir: String, val targetFile: String) extends FileSystemWrapper {

  /** Checks wherether the targetFile exists or not in stopDir.
    *
    * @return true if the file is present in stopDir false otherwise.
    */
  override def targetFileExists(): Boolean = new java.io.File(targetPath).exists

  /** Creates the targetFile into stopDir.
    *
    * @param content the string to insert.
    */
  override def createTargetFile(content: String): Unit = new java.io.PrintWriter(targetPath) {
    write(content);
    close
  }
}
