import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

// Create a Spark session
val spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()

// Load CSV file
val csvFilePath = "file:///home/parthiee/Documents/SentimentAnalysis/parsed_data.csv"
var df = spark.read.option("inferSchema", "true").csv(csvFilePath)

// Rename columns
df = df.withColumnRenamed("_c0", "text").withColumnRenamed("_c1", "target").withColumnRenamed("_c2", "emotion")

// Drop all rows with null values in any column
df = df.na.drop()

df.show()
df.printSchema()

// Tokenize the text
val tokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("words_output")
df = tokenizer.transform(df)

// Use "words_output" as the output column for further processing
val stopWordsRemover = new StopWordsRemover().setInputCol("words_output").setOutputCol("filtered_words")
df = stopWordsRemover.transform(df)

val hashingTF = new HashingTF().setInputCol("filtered_words").setOutputCol("rawFeatures").setNumFeatures(100)
df = hashingTF.transform(df)

val rf = new RandomForestClassifier().setLabelCol("target").setFeaturesCol("rawFeatures").setNumTrees(10)

val Array(newTrainingData, newTestData) = df.randomSplit(Array(0.8, 0.2), seed = 1234)

val model = rf.fit(newTrainingData)
val newPredictions = model.transform(newTestData)

newPredictions.show()

// Assuming 'newPredictions' is your DataFrame with predictions using Random Forest
val newEvaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("target")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val newAccuracy = newEvaluator.evaluate(newPredictions)

println(s"New Model Accuracy: ${newAccuracy * 100}%%")

spark.stop()
