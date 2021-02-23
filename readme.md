## This code was written and tested on the following environment:


*java version "1.8.0_221" \
Java(TM) SE Runtime Environment (build 1.8.0_221-b11) \
Java HotSpot(TM) 64-Bit Server VM (build 25.221-b11, mixed mode)\
OS name: "windows 10", version: "10.0", arch: "amd64", family: "windows"\
scala-sdk-2.11.12\
Apache Maven 3.6.2\
IDE : IntelliJ Ide 2019.2.2*


In this repository, there are code of 2 problems ( problemI and problemIII ) , Solution of problemV is written right at the end of file readme.md . \
Each time you run this code, make sure the folder input_data has only file *hash_catid_count.csv* and folder final_sort/result is empty

#### Build&Run Instructions
Step 1: Install IDE with scala plugin, maven, scala (version mentioned above) \
Step 2: 
If you are using window OS, please download `hadoop-common-2.2.0-bin-master` from https://github.com/srccodes/hadoop-common-2.2.0-bin and put it in directory `C:\ ` (Path form :`C:\hadoop-common-2.2.0-bin-master\`)\
If you are using other OS, please comment this line on ProblemI and ProblemIII code :\
 `System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")` \
Step 3:
Build and Run as normal  Java/Scala programs

    
## Problem I :
 ### I.1 & I.2
	- Using spark sql to process input data as data frames
	- Main class reference :
		khiempmk.coccoc.test.ProblemI
	- Result :
        The most popular category for this sample is 
| category_id      | sum_count |
| ----------- | ----------- |
| 15276      | 85467       |
        Category has the largest appeared times is
| category_id      | sum_count |
| ----------- | ----------- |
| 6      | 1899183       |

  ### I.3 
    We can visualize the sample in many ways, some of them as :
    -   Top 10 category by number of object_ids
    -   Top 10 most popular category and its total appeared times
    -   Distribution of Total category_counts over categories
    -   Statistics of the number of categories in a object
	
## Problem III.
	-  Because of large data, the memory is not enough to store all the data => The algorithm needs to use external memory for storage instead of buffer
	- I divide data into many partition which can be sorted by internal memory, then, sort by each partition (divide and conquer).
    - In order not to merge the result after sorting, each partition is divided so that the value domain does not intersect with the value domain of any other partition.
	    Example : Input data X = { 1,4,6,3,2,5} need to be sorted
	    The internal memory is only enough to store and sort a set of 3 elements
	    =>  Device X into 2 set: X1= {1,3,2}   (elements <= 3)
                                 X2= {4,6,5}   (elements >  3)
	    Then, sort each set independently, we have
            X1* = {1,2,3}	
			X2* = {4,5,6}
	    Append X2* intp X1* we have a sorted set of X
            X12* = {1,2,3,4,5,6}
        => No need to mix 2 set X1*, X2*
    - Propose algorith :
        + Call (L, R) is the value domain of ObjectID of the data set D (file D) under consideration
        + If  file D is too large (cannot be sorted by internal memory), dividing file D into 2 files D1, D2 satisfies:
            * All elements in D1 has objectID < M
            * All elements in D2 has objectID >= M
            * M = (L+R) / 2 
          Then recursively consider D1 and D2 respectively 
        +  Otherwise, if D is small enough, sort D and append to the output
    - Code demo :
``` 
    def sort(inputFolder : String ,left : Long , right : Long, spark : SparkSession) : Unit = {
    if (!Files.exists(Paths.get(inputFolder))) return
      if (left > right ) return
      // Read data D from Input Folder
       val df = spark.read.schema(entity_structure)
        .format("csv")
        .option("header", "false")
        .option("delimiter", "\t")
        .option("mode", "DROPMALFORMED")
        .load(inputFolder)
      
      // if data size fit the memory
      if (count < 10000){
        // Sort the data and write output
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
```

- Detailed code is available in repository, \
  Main class reference :\
*khiempmk.coccoc.test.ProblemIII*

## ProblemV
![design](https://i.ibb.co/rcmbGt8/prbV.png)
 1. Stream datasource over kafka topic
 2. Spark streaming from kafka topic
 3. With each RDD, use sparkSQL filter which object_id satisfying that rules
 3. Output is sent to kafka topic
 4. Client subcribe topic for information


 ### Pros :
1. Delay ​​can be flexibly adjusted by spark configuration or hardware scale out
3. The spark cluster doesn't directly receive data from the datasource, thus avoiding overloading when  input datasource rate is too large
4. The system can receive data from many other additional data sources
5. Easily expanded when datasource rate become larger by adding machines to spark cluster and kafka cluster
6. When one machine on cluster is down, system is still able to work normally
 ### Cons :
 1. Building the system requires experience because the component is quite complex
 2. Strategy is needed to avoid data loss


			


	
