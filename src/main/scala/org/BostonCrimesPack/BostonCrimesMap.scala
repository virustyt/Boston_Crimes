package org.BostonCrimesPack

import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

//import org.apache.log4j.Logger
//import org.apache.log4j.Level



object BostonCrimesMap extends App {

//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  val crimeCsv = args(0)

  val codesCsv = args(1)

  val parquetFilePath= args(2)

  val dfCrime = spark.read.option("header", "true").option("inferSchema", "true").csv(crimeCsv)

  val dfCode = spark.read.option("header", "true").option("inferSchema", "true").csv(codesCsv)

  import spark.implicits._

  //val BroadCodes = broadcast(dfCode)

    val crimesTotal = dfCrime
      .filter($"DISTRICT" =!= "null")
      .groupBy($"DISTRICT")
      .count()
      .withColumnRenamed("count","crimes_total")
      //.show()

dfCrime.createOrReplaceTempView("tempViewDfCrime")

  val crimesMonthly = spark.sql("select distinct DISTRICT, percentile_approx(count(1), array(0.5),100) " +
    " OVER (PARTITION BY DISTRICT, count(*) OVER (PARTITION BY DISTRICT, MONTH)) as crimesMonthly from tempViewDfCrime " +
    "where DISTRICT != '' group by DISTRICT, MONTH order by DISTRICT")

//  crimesMonthly.show()

  val frequentCrimeTypes = dfCrime
    .select($"DISTRICT",split($"OFFENSE_DESCRIPTION"," - ").getItem(0).as("OFFENSE_DESCRIPTION"))
    .where($"DISTRICT" =!= "null")
    .groupBy($"DISTRICT",$"OFFENSE_DESCRIPTION")
    .count()
    .withColumn("rank",rank().over(Window.partitionBy("DISTRICT").orderBy($"count".desc)))
    .filter($"rank" <= 3)
    .drop($"count")
    .drop($"rank")
    .withColumn("frequent_crime_types",collect_list($"OFFENSE_DESCRIPTION").over(Window.partitionBy("DISTRICT")))
    .drop($"OFFENSE_DESCRIPTION")
    .withColumn("1",array_join($"frequent_crime_types",", "))
    .drop($"frequent_crime_types")
    .withColumnRenamed("1","frequent_crime_types")
    .distinct()
    //.show(false)

  val latLng = dfCrime
    .select("DISTRICT","Lat","Long")
    .where($"DISTRICT" =!= "null")
    .withColumn("lat",avg($"Lat").over(Window.partitionBy($"DISTRICT")))
    .withColumn("lng",avg($"Long").over(Window.partitionBy($"DISTRICT")))
    .drop($"Lat")
    .drop($"Long")
    .distinct()
    //.show(false)

  val DistrictCodes = dfCrime
    .select($"DISTRICT",$"OFFENSE_CODE")

  val result = crimesTotal
    .join(crimesMonthly ,"DISTRICT")
    .join(frequentCrimeTypes, "DISTRICT")
    .join(latLng, "DISTRICT")
    .repartition(1)
    //.show(false)
    .write.parquet(parquetFilePath)

}

