I wrote everything in Scala/Spark using hive formatted tables in hadoop as the export.
scripts are setup to run on a local environment.

spark was installed on my local machine from the download here:
https://spark.apache.org/downloads.html


---HOW TO RUN SCRIPTS---
cd scripts

--Run the Script to populate the USERS table from JSON API
spark-shell --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" -i user_json_reader.scala

--Run the Script to populate the USER_GAME_INFO from CSV file input in the "../input/game_data.csv"
spark-shell --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" -i games_csv_reader.scala

--Run the Script to answer the 3 questions and export them to CSV files in the "../output" directory
spark-shell --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" -i data_questions_pull.scala
