//spark-shell --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" -i data_questions_pull.scala

import spark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,ArrayType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.expressions._



//merge csv into single output
def merge(srcPath: String, dstPath: String): Unit =  {
   new File(dstPath).delete();
   val hadoopConfig = new Configuration();
   val hdfs = FileSystem.get(hadoopConfig);
   FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null);
   // the "true" setting deletes the source files once they are merged into the new output
}

val df_users = spark.sql("select * from users");

//output to verify that users table retained UTF-8 encoding
val outputfile = "../output/";
var filename = "users.csv";
var outputFileName = outputfile + "/temp_" + filename ;
var mergedFileName = outputfile + "/merged_" + filename;
var mergeFindGlob  = outputFileName;

df_users.
  repartition(1).
  write.mode("overwrite").
  format("csv").
  option("header", true).
  option("encoding", "UTF-8").
  option("delimiter", ",").
  save(outputFileName);

merge(mergeFindGlob, mergedFileName);

val df_user_game_info = spark.sql("select * from user_game_info");

/*
1. Out of all the games, what is the percentile rank of each column used
as the first move in a game? That is, when the first player is choosing a
column for their first move, which column most frequently leads to that
player winning the game?
*/

filename = "question_1.csv";
outputFileName = outputfile + "/temp_" + filename ;
mergedFileName = outputfile + "/merged_" + filename;
mergeFindGlob  = outputFileName;

val games_total = df_user_game_info.select("game_id").distinct.count();

val df_question_1 = df_user_game_info.filter($"player_order" === 1).
  groupBy($"move_01").
  agg(
    count($"game_id").alias("games_count"),
    count(when($"result" === "win", $"game_id")).alias("games_first_player_win_count"),
    count(when($"result" === "lose", $"game_id")).alias("games_first_player_lose_count"),
    count(when($"result" === "draw", $"game_id")).alias("games_first_player_draw_count")
  ).
  orderBy("move_01").
  withColumnRenamed("move_01", "first_game_move").
  withColumn("games_total", lit(games_total)).
  withColumn("precent_games_first_player_wins", $"games_first_player_win_count"/$"games_total").
  withColumn("precent_games_second_player_wins", $"games_first_player_lose_count"/$"games_total").
  withColumn("precent_games_draw", $"games_first_player_draw_count"/$"games_total");

df_question_1.
  repartition(1).
  write.mode("overwrite").
  format("csv").
  option("header", true).
  option("encoding", "UTF-8").
  option("delimiter", ",").
  save(outputFileName);

merge(mergeFindGlob, mergedFileName);

/*
2. How many games has each nationality participated in?
*/

//notes sum of all counts is double actual games because there are two users per game and assumption of unique users/games is games that are counted

filename = "question_2.csv";
outputFileName = outputfile + "/temp_" + filename ;
mergedFileName = outputfile + "/merged_" + filename;
mergeFindGlob  = outputFileName;

val df_question_2 = df_users.join(df_user_game_info, df_users.col("id") === df_user_game_info.col("player_id"), "inner").
  select("game_id", "id", "nat").
  groupBy("nat").
  agg(
    count($"game_id").alias("games_played")
  );

df_question_2.
  repartition(1).
  write.mode("overwrite").
  format("csv").
  option("header", true).
  option("encoding", "UTF-8").
  option("delimiter", ",").
  save(outputFileName);

merge(mergeFindGlob, mergedFileName);


/*
3. Marketing wants to send emails to players that have only played a single
game. The email will be customized based on whether or not the player
won, lost, or drew the game. Which players should receive an email, and
with what customization?
*/

filename = "question_3.csv";
outputFileName = outputfile + "/temp_" + filename ;
mergedFileName = outputfile + "/merged_" + filename;
mergeFindGlob  = outputFileName;

val df_users_single_game = df_user_game_info.groupBy("player_id").
  agg(
    count($"game_id").alias("games_played"),
    max($"result").alias("result")
  ).
  filter($"games_played" === 1);

val df_question_3 = df_users.join(df_users_single_game, df_users.col("id") === df_users_single_game.col("player_id"), "inner").
  select("id", "email", "games_played", "result");


df_question_3.
  repartition(1).
  write.mode("overwrite").
  format("csv").
  option("header", true).
  option("encoding", "UTF-8").
  option("delimiter", ",").
  save(outputFileName);

merge(mergeFindGlob, mergedFileName);
