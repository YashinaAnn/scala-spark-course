package marketing_analyzer.utils

import scala.reflect.io.File

object Validator {

  def checkFileExists(path: String): Unit = {
    require(File(path).exists, s"File with path ${path} is not found")
  }

  def checkFileNotExists(path: String): Unit = {
    require(!File(path).exists, s"File with path ${path} is found")
  }
}
