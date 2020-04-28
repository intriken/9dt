//spark-shell --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" -i games_csv_reader.scala

import spark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,ArrayType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

val customSchema = StructType(Array(
  StructField("game_id", StringType, true),
  StructField("player_id", IntegerType, true),
  StructField("move_number", IntegerType, true),
  StructField("column", IntegerType, true),
  StructField("result", StringType, true)
  )
)

println("Loading Data from CSV... Starting");
val df_csv = spark.read.format("csv").option("header", "true").schema(customSchema).load("../input/game_data.csv");
println("Loading Data from CSV... Complete");

//partition by to get last result using lead in query
val w = Window.partitionBy($"game_id").orderBy($"old_result") ;

//aggregating game_id and player id information
val df_user_results = df_csv.groupBy($"game_id", $"player_id").
  agg(
    max($"result").alias("old_result"),
    max(when($"result" === "win", $"move_number")).alias("wining_move_number"),
    min(when($"move_number" === 1, lit(1)).otherwise(2)).alias("player_order")
  ).
  withColumn("other_result", lead("old_result", 1).over(w)).
  //this will get the result if missing from the other users result, if the other is a win then the current user is lose otherwise its a draw
  withColumn("result", when($"old_result".isNull, when($"other_result" === "win", lit("lose")).otherwise(lit("draw"))).otherwise($"old_result")).
  drop("old_result").
  drop("other_result");

//pivioting the move number to a single row with the 16 possible moves for each user
val df_pivot = df_csv.withColumn("move_number", format_string("%02d", $"move_number")).
  groupBy($"game_id", $"player_id").pivot("move_number").
  agg(
    max("column").alias("move_column")
  ).
  withColumnRenamed("01", "move_01").
  withColumnRenamed("02", "move_02").
  withColumnRenamed("03", "move_03").
  withColumnRenamed("04", "move_04").
  withColumnRenamed("05", "move_05").
  withColumnRenamed("06", "move_06").
  withColumnRenamed("07", "move_07").
  withColumnRenamed("08", "move_08").
  withColumnRenamed("09", "move_09").
  withColumnRenamed("10", "move_10").
  withColumnRenamed("11", "move_11").
  withColumnRenamed("12", "move_12").
  withColumnRenamed("13", "move_13").
  withColumnRenamed("14", "move_14").
  withColumnRenamed("15", "move_15").
  withColumnRenamed("16", "move_16");

//joining tables back together to have information for each user/game
val df_user_game_info = df_user_results.join(df_pivot, Seq("game_id", "player_id"), "inner");

df_user_game_info.createOrReplaceTempView("df_user_game_info");

spark.sql("drop table if exists user_game_info");

println("Writing table user_game_info ... starting");
//creating table
//TODO build out permant table structure and change to insert overwrite so that bad data does not remove existing data
spark.sql("""create table user_game_info STORED AS ORC tblproperties ("orc.compress" = "SNAPPY") as select * from df_user_game_info""");
println("Writing table user_game_info ... complete");

System.exit(0);
