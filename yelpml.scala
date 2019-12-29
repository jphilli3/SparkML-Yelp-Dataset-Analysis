package final_project
package scala

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import swiftvis2.plotting.Plot
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import breeze.stats.hist.Histogram
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.styles.PlotStyle
import swiftvis2.plotting.styles.HistogramStyle
import org.apache.spark.sql.DataFrameStatFunctions
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import scalafx.scene.effect.BlendMode.Red
import avro.shaded.com.google.common.collect.ImmutableMap
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator


object YelpML {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("Spark Final Project")
        .config("spark.driver.maxResultSize", "2g")
        .getOrCreate()
        import spark.implicits._

        spark.sparkContext.setLogLevel("WARN")

        //Yelp dataset.
        val businessData = spark.read.json("/Users/tishaphillips1/Desktop/yelp_dataset/business.json")
        val checkinData = spark.read.json("/Users/tishaphillips1/Desktop/yelp_dataset/checkin.json")
        val photoData = spark.read.json("/Users/tishaphillips1/Desktop/yelp_dataset/photo.json")
        val reviewData = spark.read.json("/Users/tishaphillips1/Desktop/yelp_dataset/review.json")
        val tipData = spark.read.json("/Users/tishaphillips1/Desktop/yelp_dataset/tip.json")
        val userData = spark.read.json("/Users/tishaphillips1/Desktop/yelp_dataset/user.json")

        //Extra dataset for cities.
        val citiesData = spark.read
        .option("header", "true")
        .option("delimiter", ";")
        .csv("/Users/tishaphillips1/Desktop/yelp_dataset/1000-largest-us-cities-by-population-with-geographic-coordinates.csv")

        //Questions
        println("Basic Questions")
        // What is the average rating for a Yelp review?
        println("QUESTION 1")
        //val averageRating = reviewData.select('stars).summary().show()

        // What is the average length per text review?
        println("QUESTION 2")
        // val reviewTexts = reviewData.select('text).cache()
        // val reviewTextsCount = reviewTexts.count()
        // val averageReviewLength = reviewTexts.map(r => r.mkString.length()).collect().foldLeft(0.0)(_+_)/reviewTextsCount
        // println(s"The average length of a text review on Yelp is ${averageReviewLength} in a dataset of ${reviewTextsCount} reviews.")

        // What city has the most Yelp reviews?
        println("QUESTION 3")
        // val reviewBusinessRenamed = reviewData.withColumnRenamed("business_id", "review_business_id")
        // val joinedBusinessReviews = businessData.join(reviewBusinessRenamed).where('business_id === 'review_business_id)
        // // val mostYelpReviews = joinedBusinessReviews.select('city).collect().groupBy(r => r).maxBy(r => r._2.length)
        // // println(s"The city with the most Yelp reviews is ${mostYelpReviews._1} with ${mostYelpReviews._2.length} reviews.")

        // val businessReviewPlotting = businessData.filter('city === "Las Vegas").select('business_id, 'latitude, 'longitude, 'stars, 'review_count).collect()
        // val reviewCG = ColorGradient(0.0 -> RedARGB, 2.5 -> YellowARGB, 5.0 -> GreenARGB)
        // //businessReviewPlotting.map(x => x(4).toString().toDouble * 0.001)
        // val reviewStyle = ScatterStyle(businessReviewPlotting.map(x => x(2).toString().toDouble), businessReviewPlotting.map(x => x(1).toString().toDouble), colors = reviewCG(businessReviewPlotting.map(x => x(3).toString().toDouble)), symbolWidth = 3.0, symbolHeight = 3.0)
        // val reviewPlot = Plot.simple(reviewStyle, "Business Reviews In Las Vegas", "Longitude", "Latutude")
        // SwingRenderer(reviewPlot,800,800,true)

        // What types of restaurants have the most Yelp reviews?
        println("QUESTION 4")
        // val businessGroupedByCategory = businessData.groupBy('categories).agg(sum("review_count").as("total_count")).orderBy(desc("total_count")).show()
        // println("Mexican restaurants have the most Yelp reviews")

        // Are there certain days of the week that more reviews are posted?
        println("QUESTION 5")
        // val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        // val daysOfTheWeek = reviewData.select('date).collect().map(r => LocalDate.parse(r(0).toString().split(' ')(0),dateFormatter).getDayOfWeek())
        // val daysGrouped = daysOfTheWeek.groupBy(x => x).map(x => (x._1,x._2.length))
        // val dayMax = daysGrouped.maxBy(x => x._2)
        // println(s"The day when the most Yelp reviews are posted is ${dayMax._1} with ${dayMax._2} reviews.")
        // Are there months that more reviews are posted?
        println("QUESTION 6")
        // val monthsOfTheYear = reviewData.select('date).collect().map(r => LocalDate.parse(r(0).toString().split(' ')(0),dateFormatter).getMonth())
        // val monthsGrouped = monthsOfTheYear.groupBy(x => x).map(x => (x._1,x._2.length))
        // val monthMax = monthsGrouped.maxBy(x => x._2)
        // println(s"The month when the most Yelp reviews are posted is ${monthMax._1} with ${monthMax._2} reviews.")
        //What is the average rating of a review that was found useful?
        println("QUESTION 7")
        //val usefulReviews = reviewData.select('useful, 'stars, 'text).where('useful > 3)
        //usefulReviews.summary().show()
        // val usefulReviewsCollected = usefulReviews.collect().map(x => (x(0).toString().toDouble,x(1).toString().toDouble,x(2).toString().length().toDouble))
        // val usefulCG = ColorGradient(0.0 -> RedARGB, 2.5 -> YellowARGB, 5.0 -> GreenARGB)
        // val usefulStyle = ScatterStyle(usefulReviewsCollected.map(x => x._3), usefulReviewsCollected.map(x => x._1), colors = usefulCG(usefulReviewsCollected.map(x => x._2)), symbolWidth = 3.0, symbolHeight = 3.0)
        // val usefulPlot = Plot.simple(usefulStyle, "Review Usefulness and Text Length", "Text Length", "Useful")
        // SwingRenderer(usefulPlot,800,800,true)

        //What is the average rating of a review that was found useful?
        println("QUESTION 8")
        // val funnyReviews = reviewData.select('funny, 'stars, 'text).where('funny > 1)
        // funnyReviews.summary().show()
        // val funnyReviewsCollected = funnyReviews.collect().map(x => (x(0).toString().toDouble,x(1).toString().toDouble,x(2).toString().length().toDouble))
        // val funnyCG = ColorGradient(0.0 -> RedARGB, 2.5 -> YellowARGB, 5.0 -> GreenARGB)
        // val funnyStyle = ScatterStyle(funnyReviewsCollected.map(x => x._3), funnyReviewsCollected.map(x => x._1), colors = funnyCG(funnyReviewsCollected.map(x => x._2)), symbolWidth = 3.0, symbolHeight = 3.0)
        // val funnyPlot = Plot.simple(funnyStyle, "Review Funniness and Text Length", "Text Length", "Funny")
        // SwingRenderer(funnyPlot,800,800,true)

        //What is the average rating of a review that was found useful?
        println("QUESTION 9")
        // val coolReviews = reviewData.select('cool, 'stars, 'text).where('cool > 1)
        // coolReviews.summary().show()
        // val coolReviewsCollected = coolReviews.collect().map(x => (x(0).toString().toDouble,x(1).toString().toDouble,x(2).toString().length().toDouble))
        // val coolCG = ColorGradient(0.0 -> RedARGB, 2.5 -> YellowARGB, 5.0 -> GreenARGB)
        // val coolStyle = ScatterStyle(coolReviewsCollected.map(x => x._3), coolReviewsCollected.map(x => x._1), colors = coolCG(coolReviewsCollected.map(x => x._2)), symbolWidth = 3.0, symbolHeight = 3.0)
        // val coolPlot = Plot.simple(coolStyle, "Review Coolness and Text Length", "Text Length", "Cool")
        // SwingRenderer(coolPlot,800,800,true)
        //ML Questions
        println("ML Questions")
        // Is there a correlation between a text reviews length and whether the review has a higher or lower rating?
        println("QUESTION 1")

        //Useful
        // val textLengthVA = new VectorAssembler().setInputCols(Array("text")).setOutputCol("features")
        // val textData = reviewData.select('stars, length('text).as("text"))
        // val textLengthTransform = textLengthVA.transform(textData)
        // textLengthTransform.show()
        // val textLengthLinReg = new LinearRegression()
        // .setFeaturesCol("features")
        // .setLabelCol("stars")
        // val textLengthModel = textLengthLinReg.fit(textLengthTransform)
        // val textLengthModelTransform = textLengthModel.transform(textLengthTransform)
        // textLengthModelTransform.show()
        // val textLengthModelTSize = textLengthModelTransform.count().toDouble
        // val textLengthRegFilter = textLengthModelTransform.select(('stars-'prediction).as[Double]).collect().map(x => math.abs(x)).sum/textLengthModelTSize
        // println(textLengthRegFilter)
        // val textLengthPlotData = textLengthModelTransform.select('stars.as[Double], 'useful.as[Double], 'text.as[Double], 'prediction.as[Double]).collect()
        // textLengthPlotData.take(5).foreach(println)
        // val cg = ColorGradient(0.0 -> GreenARGB, 0.2 -> YellowARGB, 0.5 -> RedARGB)
        // val plot = Plot.simple(ScatterStyle(textLengthPlotData.map(_._3), textLengthPlotData.map(_._2), symbolWidth = 6.0, symbolHeight = 6.0, colors = cg(textLengthPlotData.map(x => math.abs(x._1-x._4)))), "Text Length and Usefulness","Text Length","Useful")
        // SwingRenderer(plot, 800, 800, true)

        //Funny
        // val textLengthVA2 = new VectorAssembler().setInputCols(Array("text","funny")).setOutputCol("features")
        // val textData2 = reviewData.select('stars, 'funny, length('text).as("text")).where('funny > 150)
        // val textLengthTransform2 = textLengthVA2.transform(textData2)
        // textLengthTransform2.show()
        // val textLengthLinReg2 = new LinearRegression()
        // .setFeaturesCol("features")
        // .setLabelCol("stars")
        // val textLengthModel2 = textLengthLinReg2.fit(textLengthTransform2)
        // val textLengthModelTransform2 = textLengthModel2.transform(textLengthTransform2)
        // textLengthModelTransform2.show()
        // val textLengthModelTSize2 = textLengthModelTransform2.count().toDouble
        // val textLengthRegFilter2 = textLengthModelTransform2.select(('stars-'prediction).as[Double]).collect().map(x => math.abs(x)).sum/textLengthModelTSize2
        // println(textLengthRegFilter2)
        // val textLengthPlotData2 = textLengthModelTransform2.select('stars.as[Double], 'funny.as[Double], 'text.as[Double], 'prediction.as[Double]).collect()
        // textLengthPlotData2.take(5).foreach(println)
        // val cg2 = ColorGradient(0.0 -> GreenARGB, 0.2 -> YellowARGB, 0.5 -> RedARGB)
        // val plot2 = Plot.simple(ScatterStyle(textLengthPlotData2.map(_._3), textLengthPlotData2.map(_._2), symbolWidth = 6.0, symbolHeight = 6.0, colors = cg2(textLengthPlotData2.map(x => math.abs(x._1-x._4)))), "Text Length and Funniness","Text Length","Funny")
        // SwingRenderer(plot2, 800, 800, true)

        //Cool
        // val textLengthVA3 = new VectorAssembler().setInputCols(Array("text","cool")).setOutputCol("features")
        // val textData3 = reviewData.select('stars, 'cool, length('text).as("text")).where('cool > 150)
        // val textLengthTransform3 = textLengthVA3.transform(textData3)
        // textLengthTransform3.show()
        // val textLengthLinReg3 = new LinearRegression()
        // .setFeaturesCol("features")
        // .setLabelCol("stars")
        // val textLengthModel3 = textLengthLinReg3.fit(textLengthTransform3)
        // val textLengthModelTransform3 = textLengthModel3.transform(textLengthTransform3)
        // textLengthModelTransform3.show()
        // val textLengthModelTSize3 = textLengthModelTransform3.count().toDouble
        // val textLengthRegFilter3 = textLengthModelTransform3.select(('stars-'prediction).as[Double]).collect().map(x => math.abs(x)).sum/textLengthModelTSize3
        // println(textLengthRegFilter3)
        // val textLengthPlotData3 = textLengthModelTransform3.select('stars.as[Double], 'cool.as[Double], 'text.as[Double], 'prediction.as[Double]).collect()
        // textLengthPlotData3.take(5).foreach(println)
        // val cg3 = ColorGradient(0.0 -> GreenARGB, 0.2 -> YellowARGB, 0.5 -> RedARGB)
        // val plot3 = Plot.simple(ScatterStyle(textLengthPlotData3.map(_._3), textLengthPlotData3.map(_._2), symbolWidth = 6.0, symbolHeight = 6.0, colors = cg3(textLengthPlotData3.map(x => math.abs(x._1-x._4)))), "Text Length and Coolness","Text Length","Cool")
        // SwingRenderer(plot3, 800, 800, true)

        //Useful, Funny, Cool

        // val textLengthVA4 = new VectorAssembler().setInputCols(Array("text","useful","funny","cool")).setOutputCol("features")
        // val textData4 = reviewData.select('stars,'useful, 'funny, 'cool, length('text).as("text")).where('cool > 150 && 'funny > 150 && 'useful > 150)
        // val textLengthTransform4 = textLengthVA4.transform(textData4)
        // textLengthTransform4.show()
        // val textLengthLinReg4 = new LinearRegression()
        // .setFeaturesCol("features")
        // .setLabelCol("stars")
        // val textLengthModel4 = textLengthLinReg4.fit(textLengthTransform4)
        // val textLengthModelTransform4 = textLengthModel4.transform(textLengthTransform4)
        // textLengthModelTransform4.show()
        // val textLengthModelTSize4 = textLengthModelTransform4.count().toDouble
        // val textLengthRegFilter4 = textLengthModelTransform4.select(('stars-'prediction).as[Double]).collect().map(x => math.abs(x)).sum/textLengthModelTSize4
        // println(textLengthRegFilter4)
        //  val textLengthPlotData = textLengthModelTransform4.select('stars.as[Double], 'text.as[Double], 'prediction.as[Double]).collect()
        // textLengthPlotData.take(5).foreach(println)
        // val cg = ColorGradient(0.0 -> GreenARGB, 0.2 -> YellowARGB, 0.5 -> RedARGB)
        // val plot = Plot.simple(ScatterStyle(textLengthPlotData.map(_._1), textLengthPlotData.map(_._2), symbolWidth = 10.0, symbolHeight = 10.0, colors = cg(textLengthPlotData.map(x => math.abs(x._1-x._3)))), "Text Length, Usefulness, Funniness, and Coolness Regression","Rating","Text Length")
        // SwingRenderer(plot, 800, 800, true)

        // var cols = Set("useful","cool","funny")

        // val params = cols.subsets(2)
        // var errorArr = Array.empty[(Double,Seq[String])]
        // for (subset <- params) {

        // val one = subset.toSeq(0)
        // val two = subset.toSeq(1)

        // val textLengthVA = new VectorAssembler().setInputCols(subset.toArray).setOutputCol("features")
        // val textData = reviewData.select("stars", one, two)
        // val textLengthTransform = textLengthVA.transform(textData)
        // val textLengthLinReg = new LinearRegression()
        // .setFeaturesCol("features")
        // .setLabelCol("stars")
        // val textLengthModel = textLengthLinReg.fit(textLengthTransform)
        // val textLengthModelTransform = textLengthModel.transform(textLengthTransform)
        // val textLengthModelTSize = textLengthModelTransform.count().toDouble
        // val textLengthRegFilter = textLengthModelTransform.select(('stars-'prediction).as[Double]).collect().map(x => math.abs(x)).sum/textLengthModelTSize
        // println(textLengthRegFilter, subset.toSeq)

        // }

        // val textLengthVA4 = new VectorAssembler().setInputCols(Array("useful","funny","cool")).setOutputCol("features")
        // val textData4 = reviewData.select('stars,'useful, 'funny, 'cool).where('cool > 150 && 'funny > 150 && 'useful > 150)
        // val textLengthTransform4 = textLengthVA4.transform(textData4)
        // textLengthTransform4.show()
        // val textLengthLinReg4 = new LinearRegression()
        // .setFeaturesCol("features")
        // .setLabelCol("stars")
        // val textLengthModel4 = textLengthLinReg4.fit(textLengthTransform4)
        // val textLengthModelTransform4 = textLengthModel4.transform(textLengthTransform4)
        // textLengthModelTransform4.show()
        // val textLengthModelTSize4 = textLengthModelTransform4.count().toDouble
        // val textLengthRegFilter4 = textLengthModelTransform4.select(('stars-'prediction).as[Double]).collect().map(x => math.abs(x)).sum/textLengthModelTSize4
        // println(textLengthRegFilter4)
        // val textDataSelected = reviewData.select(length('text).as("text"), 'stars,'useful, 'funny, 'cool).where('cool > 150 && 'funny > 150 && 'useful > 150)
        // Split the data into training and test sets (30% held out for testing)

        // val va = new VectorAssembler()
        // .setInputCols(Array("stars","text","useful","funny","cool"))
        // .setOutputCol("features")
        // .transform(textDataSelected)
        // val scalar = new StandardScaler().setInputCol("selected").setOutputCol("features").fit(va).transform(va)
    
        // val kmeans = new KMeans().setK(5)
        // val kmeansModel = kmeans.fit(scalar)
        // val dataWithClusters = kmeansModel.transform(scalar)
        // val textLengthRegFilter5 = dataWithClusters.select(('stars-'prediction).as[Double]).collect().map(x => math.abs(x)).sum/dataWithClusters.count().toDouble
        // println(textLengthRegFilter5)
        // val pdata = dataWithClusters.select('stars.as[Double], 'text.as[Double], 'prediction.as[Double]).collect()
        // val cg2 = ColorGradient(0.0 -> GreenARGB, 2.0 -> YellowARGB, 4.0 -> RedARGB)
        // val plot2 = Plot.simple(ScatterStyle(pdata.map(_._1), pdata.map(_._2), colors = cg2(pdata.map(x => math.abs(x._1 - x._3)))), "Text Length, Usefulness, Funniness, and Coolness KMeans", "Rating", "Text Length")
        // SwingRenderer(plot2, 800, 800, true)

        spark.sparkContext.stop()
        spark.stop()
    }
}
