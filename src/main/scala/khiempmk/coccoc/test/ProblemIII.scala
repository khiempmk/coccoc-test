package khiempmk.coccoc.test

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{LongType, StructType}

object ProblemIII {

  val entity_structure = new StructType()
    .add("object_id", LongType)
    .add("category_ids", "string")
    .add("category_counts", "string")
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val spark = SparkSession.builder
      .appName("CoccocTest3")
      .master("local[2]")
      .getOrCreate()


    sort("input_data\\", 0L, Long.MaxValue, spark);

    // read the sorted output folder
    val df = spark.read.schema(entity_structure)
      .format("csv")
      .option("delimiter", "\t")
      .load("final_sort\\result")
    // show the sorted output df
    df.show()
    print("SortP is completed successfully")

  }
  // Sort demo algorithm
  def sort(inputFolder : String ,left : Long , right : Long, spark : SparkSession) : Unit = {
    if (!Files.exists(Paths.get(inputFolder))) return
      if (left > right ) return
      val df = spark.read.format("csv").option("header", "false")
         .schema(entity_structure) // define schema for dataframe
        .option("delimiter", "\t")  // define delimiter of csv source file
        .option("mode", "DROPMALFORMED")  // ignore all  null row
        .load(inputFolder)
      val count = df.count() ;
      print("Working .. " +left + " " + right + " " + count)
      // if data size fit the memory
      // Sort the data and write output
      if (count < 10000){
        val df1 = df.sort(col("object_id")).repartition(1).write.mode("append")
          .option("sep","\t")
          .csv("final_sort/result")
      } else {
        // Otherwise the data size is too big, can't not be sort with our limited memory
        // Separate data into 2 sub-parts
        // Fist part (left part ) contain all elements have object_id < mid
        // Second part (right_path) contain all elements have object_id >= mid
        val mid = left /2 +  right /2
        df.withColumn("class", when(col("object_id") < mid , 0).otherwise(1))
          .write.mode("append")
          .option("sep","\t")
          .partitionBy("class")   // Separate using partitionBy
          .csv(inputFolder)
        // Sort left part
        sort(inputFolder + "class=0\\", left, mid-1,spark);
        // Sort right part
        sort(inputFolder + "class=1\\", mid, right,spark);
      }

}
}
