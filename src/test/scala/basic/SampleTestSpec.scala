package basic

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class SampleTestSpec extends FunSuite with BeforeAndAfterEach {

  private val master = "local"

  private val appName = "ReadFileTest"

  var spark : SparkSession = _

  override def beforeEach(): Unit = {
    spark = new sql.SparkSession.Builder().appName(appName).master(master).getOrCreate()
  }

  test("creating data frame from text file") {
    val sparkSession = spark
    val filePath="src/test/resources/text/data.txt"
    val df = spark.read.text(filePath)
    df.printSchema()
  }

  /*

  test("counts should match with number of records in a text file") {
    val sparkSession = spark
    import sparkSession.implicits._
    val peopleDF = ReadAndWrite.readFile(sparkSession,"src/test/resources/people.txt").map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toInt)).toDF()

    peopleDF.printSchema()
    assert(peopleDF.count() == 3)
  }

  test("data should match with sample records in a text file") {
    val sparkSession = spark
    import sparkSession.implicits._
    val peopleDF = ReadAndWrite.readFile(sparkSession,"src/test/resources/people.txt").map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toInt)).toDF()

    peopleDF.printSchema()
    assert(peopleDF.take(1)(0)(0).equals("Michael"))
  }

  test("Reading files of different format using readTextfileToDataSet should throw an exception") {

    intercept[org.apache.spark.sql.AnalysisException] {
      val sparkSession = spark
      import org.apache.spark.sql.functions.col

      val df = ReadAndWrite.readFile(sparkSession,"src/test/resources/people.parquet")
      df.select(col("name"))

    }
  }

  test("Reading an invalid file location using readTextfileToDataSet should throw an exception") {

    intercept[Exception] {
      val sparkSession = spark
      import org.apache.spark.sql.functions.col
      val df = ReadAndWrite.readFile(sparkSession,"src/test/resources/invalid.txt")

      df.show()

    }
  }
*/
  override def afterEach(): Unit = {
    spark.stop()
  }
}

