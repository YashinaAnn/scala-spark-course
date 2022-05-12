package marketing_analyzer.utils

import org.apache.spark.sql.Dataset

object DataWriter {

  def saveDS[T](path: String, ds: Dataset[T]): Unit = {
    ds.write.parquet(path)
  }

  def savePartitionedDS[T](path: String, ds: Dataset[T], columns: Seq[String]): Unit = {
    ds.write
      .partitionBy(columns: _*)
      .parquet(path)
  }
}
