package khiempmk.coccoc.test

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ProblemI {
  def main(args: Array[String]): Unit = {
    // Uncomment if using WindowsOS
    //System.setProperty("hadoop.home.dir", "C:/hadoop-common-2.2.0-bin-master/")
    val session = SparkSession.builder()
      .appName("CoccocTest1")
      .master("local[2]")
      .getOrCreate()

    val arrayStructureSchema = new StructType()
      .add("object_id", StringType)
      .add("category_ids",StringType)
      .add("category_counts",StringType )
    val df = session.read.format("csv")
                    .option("header", "false")
                    .option("delimiter", "\t")        // define delimiter of csv source file
                    .schema(arrayStructureSchema)     // define schema for dataframe
                    .option("mode", "DROPMALFORMED")  // ignore all  null row
                    .load("input_data/hash_catid_count.csv")
    df.printSchema()
    // category_ids and  category_counts are in String type, we need cast them to array type
    // Create new dataframe by add new columns with array<long> type of current string columns(category_ids, category_counts)
    val df1 = df.withColumn("category_id_list", split(regexp_replace(col("category_ids"),"[\\[\\]]",""), ",").cast("array<long>"))
                .withColumn("category_count_list", split(regexp_replace(col("category_counts"),"[\\[\\]]",""), ",").cast("array<long>"))
    df1.show()

    import session.implicits._
    // Solver 1A question
    // Separate category_id_list by flatmap,
    // Using count column, calculate sum_count for each category_id
    // Get row has max(sum_count) for the answer
    val df2=df1.flatMap(f=> f.getSeq[Long](3)).withColumn("count",lit(1))
      .toDF("category_id","count")
    val df3 = df2.groupBy("category_id").agg(sum($"count") as "sum_count")
    // Show the answer

    df3.orderBy(desc("sum_count")).limit(1).show()
    print("The most popular category for this sample is above \n")
    // Solver 1B question
    // Use flatmap with zip to tranform origin df to new df has two col : category_id and count ( count is appeared times in each object)
    // Using count column, calculate sum_count for each category_id
    // Get row has max(sum_count) for the answer
    val df5 = df1.flatMap(f => tranfer(f)).toDF("category_id", "count")
    val df7 = df5.groupBy("category_id").agg(sum($"count") as "sum_count")
    // Show the answer
    df7.orderBy(desc("sum_count")).limit(1).show()
    print("Category has the largest appeared times is above \n ")

  }

  // Function using by flatmap
  // Tranform a row into sequence of row using zip of two list ( category_id & category_count_list)
  def tranfer(row: Row): Seq[(Long,Long)] ={
      val cs = row.getSeq[Long](3)
      val cy = row.getSeq[Long](4)
      val bs = cs zip cy
      bs

  }
}
