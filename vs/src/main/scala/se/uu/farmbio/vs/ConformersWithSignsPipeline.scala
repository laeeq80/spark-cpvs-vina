package se.uu.farmbio.vs

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.commons.io.FilenameUtils
import org.openscience.cdk.io.SDFWriter
import java.sql.{ DriverManager, PreparedStatement }

import org.apache.spark.storage.StorageLevel
import java.io.PrintWriter
import java.nio.file.Paths

import se.uu.it.cp
import se.uu.it.cp.{ ICP, InductiveClassifier }

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream, StringWriter }

trait ConformersWithSignsTransforms {
  def dockWithML(
    receptorPath:       String,
    pdbCode:            String,
    jdbcHostname:       String,
    dsInitSize:         Int,
    dsIncreSize:        Int,
    calibrationPercent: Double,
    numIterations:      Int,
    badIn:              Int,
    goodIn:             Int,
    singleCycle:        Boolean,
    stratified:         Boolean,
    confidence:         Double): SBVSPipeline with PoseTransforms
}

object ConformersWithSignsPipeline extends Serializable {

  private def getLPRDD(poses: String) = {
    val it = SBVSPipeline.CDKInit(poses)

    var res = Seq[(LabeledPoint)]()

    while (it.hasNext()) {
      //for each molecule in the record compute the signature

      val mol = it.next
      val label: String = mol.getProperty("Label")
      val doubleLabel: Double = label.toDouble
      val labeledPoint = new LabeledPoint(doubleLabel, Vectors.parse(mol.getProperty("Signature")))
      res = res ++ Seq(labeledPoint)
    }

    res //return the labeledPoint
  }

  private[vs] def getFeatureVector(poses: String) = {
    val it = SBVSPipeline.CDKInit(poses)

    var res = Seq[(Vector)]()

    while (it.hasNext()) {
      //for each molecule in the record compute the FeatureVector
      val mol = it.next
      val Vector = Vectors.parse(mol.getProperty("Signature"))
      res = res ++ Seq(Vector)
    }

    res //return the FeatureVector
  }

  private def labelTopAndBottom(
    pdbqtRecord:    String,
    score:          Double,
    scoreHistogram: Array[Double],
    badIn:          Int,
    goodIn:         Int) = {
    val it = SBVSPipeline.CDKInit(pdbqtRecord)
    val strWriter = new StringWriter()
    val writer = new SDFWriter(strWriter)
    while (it.hasNext()) {
      val mol = it.next
      val label = score match { //convert labels
        case score if score >= scoreHistogram(0) && score <= scoreHistogram(goodIn) => 1.0
        case score if score >= scoreHistogram(badIn) && score <= scoreHistogram(10) => 0.0
        case _ => "NAN"
      }

      if (label == 0.0 || label == 1.0) {
        mol.removeProperty("cdk:Remark")
        mol.setProperty("Label", label)
        writer.write(mol)
      }
    }
    writer.close
    strWriter.toString() //return the molecule
  }

  private def getLabel(sdfRecord: String) = {

    val it = SBVSPipeline.CDKInit(sdfRecord)
    var label: String = null
    while (it.hasNext()) {
      val mol = it.next
      label = mol.getProperty("Label")

    }
    label

  }

  private def insertModels(receptorPath: String, rModel: InductiveClassifier[MLlibSVM, LabeledPoint], rPdbCode: String, jdbcHostname: String) {
    //Getting filename from Path and trimming the extension
    val rName = FilenameUtils.removeExtension(Paths.get(receptorPath).getFileName.toString())
    println("JOB_INFO: The value of rName is " + rName)

    Class.forName("org.mariadb.jdbc.Driver")
    val jdbcUrl = s"jdbc:mysql://" + jdbcHostname + ":3306/db_profile?user=root&password=2264421_root"

    //Preparation object for writing
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(rModel)

    val rModelAsBytes = baos.toByteArray()
    val bais = new ByteArrayInputStream(rModelAsBytes)

    val connection = DriverManager.getConnection(jdbcUrl)
    if (!(connection.isClosed())) {
      //Writing to Database
      val sqlInsert: PreparedStatement = connection.prepareStatement("INSERT INTO MODELS(r_name, r_pdbCode, r_model) VALUES (?, ?, ?)")

      println("JOB_INFO: Start Serializing")

      // set input parameters
      sqlInsert.setString(1, rName)
      sqlInsert.setString(2, rPdbCode)
      sqlInsert.setBinaryStream(3, bais, rModelAsBytes.length)
      sqlInsert.executeUpdate()

      sqlInsert.close()
      println("JOB_INFO: Done Serializing")

    } else {
      println("MariaDb Connection is Close")
      System.exit(1)
    }
  }

  private def insertPredictions(receptorPath: String, rPdbCode: String, jdbcHostname: String, predictions: RDD[(String, Set[Double])], sc: SparkContext) {
    //Reading receptor name from path
    val rName = FilenameUtils.removeExtension(Paths.get(receptorPath).getFileName.toString())

    //Getting parameters ready in Row format
    val paramsAsRow = predictions.map {
      case (sdfmol, predSet) =>
        val lId = PosePipeline.parseId(sdfmol)
        val lPrediction = if (predSet == Set(0.0)) "BAD"
        else if (predSet == Set(1.0)) "GOOD"
        else "UNKNOWN"
        (lId, lPrediction)
    }.map {
      case (lId, lPrediction) =>
        Row(rName, rPdbCode, lId, lPrediction)
    }

    //Creating sqlContext Using sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val schema =
      StructType(
        StructField("r_name", StringType, false) ::
          StructField("r_pdbCode", StringType, false) ::
          StructField("l_id", StringType, false) ::
          StructField("l_prediction", StringType, false) :: Nil)

    //Creating DataFrame using row parameters and schema
    val df = sqlContext.createDataFrame(paramsAsRow, schema)

    val prop = new java.util.Properties
    prop.setProperty("driver", "org.mariadb.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "2264421_root")

    //jdbc mysql url - destination database is named "db_profile"
    val url = "jdbc:mysql://" + jdbcHostname + ":3306/db_profile"

    //destination database table
    val table = "PREDICTED_LIGANDS"

    //write data from spark dataframe to database
    df.write.mode("append").jdbc(url, table, prop)
    df.printSchema()
  }

}

private[vs] class ConformersWithSignsPipeline(override val rdd: RDD[String])
  extends SBVSPipeline(rdd) with ConformersWithSignsTransforms {

  override def dockWithML(
    receptorPath:       String,
    pdbCode:            String,
    jdbcHostname:       String,
    dsInitSize:         Int,
    dsIncreSize:        Int,
    calibrationPercent: Double,
    numIterations:      Int,
    badIn:              Int,
    goodIn:             Int,
    singleCycle:        Boolean,
    stratified:         Boolean,
    confidence:         Double) = {

    //initializations
  
    var ds: RDD[String] = rdd.flatMap(SBVSPipeline.splitSDFmolecules).persist(StorageLevel.MEMORY_AND_DISK_SER)
    
      val dsInitToDock = ds.mapPartitions(x => Seq(x.mkString("\n")).iterator)

      //Step 2
      //Docking the sampled dataset
      val dsDock = ConformerPipeline
        .getDockingRDD(receptorPath, dockTimePerMol = false, sc, dsInitToDock, true)
        .map {
          case (dirtyMol) => ConformerPipeline.cleanPoses(dirtyMol, true)
        }
        .flatMap(SBVSPipeline.splitSDFmolecules).persist(StorageLevel.DISK_ONLY)

    new PosePipeline(dsDock)
  }

}