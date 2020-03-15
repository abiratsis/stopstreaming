package com.abiratsis.spark.streaming.extensions.fs

trait FileSystemWrapper {
  val stopDir: String
  val targetFile: String
  val targetPath = s"${stopDir}/${targetFile}"

  /** Checks wherether the targetFile exists or not in stopDir.
    *
    * @return true if the file is present in stopDir false otherwise.
   */
  def targetFileExists() : Boolean

  /** Creates the targetFile into stopDir.
    *
    *  @param content the string to insert.
    */
  def createTargetFile(content: String) : Unit
}
