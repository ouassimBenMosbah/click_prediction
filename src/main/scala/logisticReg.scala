import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.DataFrame
import scala.util.matching.Regex
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, when, col}

object PredictClick {
  /* ----------------------------------------- */
  /* -------------- Preprocessing ------------ */
  /* ----------------------------------------- */
  def preprocess(df1: DataFrame, t: Boolean): DataFrame = {
    val network = udf {(str: String) =>
        val fr = new Regex("208-(.*)")
        val can = new Regex("302-(.*)")
        val es = new Regex("214-(.*)")
        val tur = new Regex("286-(.*)")
        val ger = new Regex("262-(.*)")

        str match {
        case fr(x) => "France"
        case null | "Other" | "Unknown" => "Unknown"
        case can(x) => "Canada"
        case es(x) => "Espagne"
        case tur(x) => "Turquie"
        case ger(x) => "Allemagne"
        case _ => "Unknown"
        }
    }

    val os = udf {(str: String) =>
      val windows_pattern = new Regex("Windows(.*)")
      
      str match {
      case windows_pattern(x) => "Windows Phone"
      case null | "Other" | "Unknown" | "Rim" | "Bada" => "Unknown"
      case x => x.toLowerCase.capitalize
      }
    }

    def epochToDateUDF = udf((current_time : Long)  =>{
      DateTimeFormat.forPattern("HH").print(current_time *1000).toInt
    })

    def classificateDate= udf((date:Int)=>{
      date match{
        case x if (x > 6 && x < 14) => "Morning"
        case x if (x >= 14 && x < 22) => "Afternoon"
        case _ => "Night"
      }
    })

    val df2 = if (t) 
      {df1.withColumn("bidfloor", when(col("bidfloor").isNull, 3).otherwise(col("bidfloor"))).withColumn("label", when(col("label") === true, 1).otherwise(0)).withColumn("os", os(df1("os"))).withColumn("network", network(df1("network"))).withColumn("timestamp", classificateDate(epochToDateUDF(df1("timestamp"))))} 
    else 
      {df1.withColumn("bidfloor", when(col("bidfloor").isNull, 3).otherwise(col("bidfloor"))).withColumn("os", os(df1("os"))).withColumn("network", network(df1("network"))).withColumn("timestamp", classificateDate(epochToDateUDF(df1("timestamp"))))}

    // ### transform all categorical data into stringIndexer and then in OHE and finally add them to df
    val categoricals = df2.dtypes.filter (_._2 == "StringType")map (_._1)

    val indexers = categoricals.map (
      c => new StringIndexer().setInputCol(c).setOutputCol(s"${c}_idx")
    )

    val encoders = categoricals.map (
      c => new OneHotEncoder().setInputCol(s"${c}_idx").setOutputCol(s"${c}_enc")
    )

    val pipeline = new Pipeline().setStages(indexers ++ encoders)
    val df3 = pipeline.fit(df2).transform(df2)

    val assembler = new VectorAssembler().setInputCols(Array("appOrSite_enc", "bidfloor", "media_enc", "os_enc", "publisher_enc","exchange_enc","network_enc","timestamp_enc")).setOutputCol("features")
    //return a dataframe with all of the  feature columns in  a vector column**
    if (t) {
      assembler.transform(df3).select("appOrSite", "bidfloor", "city", "exchange", "impid", "interests", "label", "media", "network", "os", "publisher","timestamp", "type", "user", "features")
    } else {
      assembler.transform(df3).select("appOrSite", "bidfloor", "city", "exchange", "impid", "interests", "media", "network", "os", "publisher","timestamp", "type", "user", "features")
    }
  }

  def balanceDataset(dataset: DataFrame): DataFrame = {
    // Re-balancing (weighting) of records to be used in the logistic loss objective function
    // This function improve the weight of the true label while it decrease the weight of false label
    val numNegatives = dataset.filter(dataset("label") === 0).count
    val datasetSize = dataset.count
    val balancingRatio = (datasetSize - numNegatives).toDouble / datasetSize

    val calculateWeights = udf { d: Double =>
      if (d == 0.0) {
        1 * balancingRatio
      }
      else {
        (1 * (1.0 - balancingRatio))
      }
    }

    val weightedDataset = dataset.withColumn("classWeightCol", calculateWeights(dataset("label")))
    weightedDataset
  }

  /* ------------------------------------------------------ */ 
  /* ------------------------ MAIN ------------------------ */
  /* ------------------------------------------------------ */

  def main(args: Array[String]) {

    implicit val spark: SparkSession = SparkSession.builder().appName("click_prediction").master("local[*]").getOrCreate()

    /* ----------------------------------------- */
    /* ------------ Getting the data ----------- */
    /* ----------------------------------------- */
    val train = spark.read.json("data-students.json")


    /* ----------------------------------------- */
    /* ---------- Getting train data ----------- */
    /* ----------------------------------------- */
    val trainingDataUnbalanced = preprocess(train, true)
    val trainingData = balanceDataset(trainingDataUnbalanced.select("label", "features"))


    /* ----------------------------------------- */
    /* ----------- Train the model ------------- */
    /* ----------------------------------------- */
    val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features").setWeightCol("classWeightCol").setMaxIter(10).setThreshold(0.4)
    /* use logistic regression to train (fit) the model with the training data */
    val lrModel = lr.fit(trainingData.select("label", "features", "classWeightCol"))


    /* ----------------------------------------- */
    /* ----------- Do the predictions ---------- */
    /* ----------------------------------------- */
    val test = spark.read.json("test.json")
    val testData = preprocess(test, false).select("appOrSite", "bidfloor", "city", "exchange", "impid", "interests", "media", "network", "os", "publisher","timestamp", "type", "user", "features")
    val predictions = lrModel.transform(testData).select("prediction", "appOrSite", "bidfloor", "city", "exchange", "impid", "interests", "media", "network", "os", "publisher", "timestamp", "type", "user")


    /* ----------------------------------------- */
    /* ------------ Export to CSV -------------- */
    /* ----------------------------------------- */
    predictions.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("result_predictions")
  }
}