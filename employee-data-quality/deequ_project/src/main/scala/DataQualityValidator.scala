import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.analyzers._
import com.amazon.deequ.metrics._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import java.io.{File, PrintWriter}
import scala.util.{Try, Success, Failure}
import scala.io.Source
import play.api.libs.json._

object DataQualityValidator {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("Employee Data Quality Validation")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    val datasetPath = "/app/dataset"
    val outputPath = "/app/output"
    
    println("Starting Deequ validation process...")
    
    // Get all CSV files
    val csvFiles = new File(datasetPath).listFiles()
      .filter(_.getName.endsWith(".csv"))
      .toList
    
    csvFiles.foreach { file =>
      val tableName = file.getName.replace(".csv", "")
      println(s"Processing table: $tableName")
      
      try {
        val df = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(file.getAbsolutePath)
        
        // Run metrics analysis
        val metricsResult = runMetricsAnalysis(df, tableName)
        saveMetricsToJson(metricsResult, tableName, outputPath)
        
        // Run validation checks
        val validationResult = runValidationChecks(df, tableName)
        saveValidationToJson(validationResult, tableName, outputPath)
        
        println(s"Completed processing for $tableName")
        
      } catch {
        case e: Exception =>
          println(s"Error processing $tableName: ${e.getMessage}")
          e.printStackTrace()
      }
    }
    
    spark.stop()
    println("Deequ validation completed.")
  }
  
  def runMetricsAnalysis(df: DataFrame, tableName: String): AnalyzerContext = {
    val columns = df.columns.toList
    
    val analyzers = List(
      Size(),
      Completeness("*")
    ) ++ columns.flatMap { col =>
      val colType = df.schema(col).dataType
      
      val baseAnalyzers = List(
        Completeness(col),
        Distinctness(col)
      )
      
      val typeSpecificAnalyzers = colType match {
        case _: NumericType =>
          List(
            Minimum(col),
            Maximum(col),
            Mean(col),
            StandardDeviation(col)
          )
        case _: StringType =>
          List(
            MinLength(col),
            MaxLength(col),
            Uniqueness(col)
          )
        case _ => List.empty
      }
      
      baseAnalyzers ++ typeSpecificAnalyzers
    }
    
    println(s"Running ${analyzers.length} analyzers for $tableName")
    
    AnalysisRunner
      .onData(df)
      .addAnalyzers(analyzers)
      .run()
  }
  
  def runValidationChecks(df: DataFrame, tableName: String): VerificationResult = {
    val columns = df.columns.toList
    
    // Create table-specific checks
    val checks = tableName match {
      case "employees" =>
        List(
          Check(CheckLevel.Error, "employees_checks")
            .hasSize(_ > 0)
            .isComplete("emp_no")
            .isUnique("emp_no")
            .isComplete("first_name")
            .isComplete("last_name")
            .isContainedIn("gender", Array("M", "F"))
        )
      
      case "salaries" =>
        List(
          Check(CheckLevel.Error, "salaries_checks")
            .hasSize(_ > 0)
            .isComplete("emp_no")
            .isComplete("salary")
            .satisfies("salary > 0", "Salary should be positive")
        )
      
      case "titles" =>
        List(
          Check(CheckLevel.Error, "titles_checks")
            .hasSize(_ > 0)
            .isComplete("emp_no")
            .isComplete("title")
        )
      
      case "departments" =>
        List(
          Check(CheckLevel.Error, "departments_checks")
            .hasSize(_ > 0)
            .isComplete("dept_no")
            .isUnique("dept_no")
            .isComplete("dept_name")
            .isUnique("dept_name")
        )
      
      case "dept_emp" =>
        List(
          Check(CheckLevel.Error, "dept_emp_checks")
            .hasSize(_ > 0)
            .isComplete("emp_no")
            .isComplete("dept_no")
        )
      
      case "dept_manager" =>
        List(
          Check(CheckLevel.Error, "dept_manager_checks")
            .hasSize(_ > 0)
            .isComplete("emp_no")
            .isUnique("emp_no")
            .isComplete("dept_no")
        )
      
      case _ =>
        List(
          Check(CheckLevel.Error, "default_checks")
            .hasSize(_ > 0)
        )
    }
    
    println(s"Running ${checks.length} validation checks for $tableName")
    
    VerificationSuite()
      .onData(df)
      .addChecks(checks)
      .run()
  }
  
  def saveMetricsToJson(metricsResult: AnalyzerContext, tableName: String, outputPath: String): Unit = {
    try {
      val metrics = metricsResult.allMetrics
      
      val jsonMetrics = metrics.map { case (analyzer, metric) =>
        val metricValue = metric match {
          case DoubleMetric(_, _, _, value, _) => value.toString
          case _ => metric.toString
        }
        
        Json.obj(
          "analyzer" -> analyzer.toString,
          "value" -> metricValue,
          "metric_type" -> metric.getClass.getSimpleName
        )
      }.toList
      
      val output = Json.obj(
        "table_name" -> tableName,
        "timestamp" -> java.time.Instant.now().toString,
        "metrics" -> jsonMetrics
      )
      
      val outputFile = new File(s"$outputPath/${tableName}_deequ_metrics.json")
      outputFile.getParentFile.mkdirs()
      
      val writer = new PrintWriter(outputFile)
      writer.write(Json.prettyPrint(output))
      writer.close()
      
      println(s"Metrics saved to ${outputFile.getAbsolutePath}")
      
    } catch {
      case e: Exception =>
        println(s"Error saving metrics for $tableName: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  def saveValidationToJson(validationResult: VerificationResult, tableName: String, outputPath: String): Unit = {
    try {
      val checkResults = validationResult.checkResults.map { case (check, checkResult) =>
        Json.obj(
          "check_name" -> check.description,
          "status" -> checkResult.status.toString,
          "constraints" -> checkResult.constraintResults.map { constraintResult =>
            Json.obj(
              "constraint" -> constraintResult.constraint.toString,
              "status" -> constraintResult.status.toString,
              "message" -> constraintResult.message.getOrElse("")
            )
          }
        )
      }.toList
      
      val output = Json.obj(
        "table_name" -> tableName,
        "timestamp" -> java.time.Instant.now().toString,
        "overall_status" -> validationResult.status.toString,
        "check_results" -> checkResults
      )
      
      val outputFile = new File(s"$outputPath/${tableName}_deequ_validation.json")
      outputFile.getParentFile.mkdirs()
      
      val writer = new PrintWriter(outputFile)
      writer.write(Json.prettyPrint(output))
      writer.close()