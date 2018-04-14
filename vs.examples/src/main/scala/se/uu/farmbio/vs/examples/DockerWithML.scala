package se.uu.farmbio.vs.examples

import org.apache.spark.{ Logging, SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import scopt.OptionParser
import se.uu.farmbio.vs.{ SBVSPipeline, PosePipeline, ConformersWithSignsPipeline }

import org.apache.hadoop.io.{ LongWritable, Text }
import se.uu.farmbio.parsers.SDFInputFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.nio.file.Paths

import org.apache.commons.io.FilenameUtils

/**
 * @author laeeq
 */

object DockerWithML extends Logging {

  case class Arglist(
    master:              String  = null,
    conformersFile:      String  = null,
    sig2IdPath:          String  = null,
    topPosesPath:        String  = null,
    receptorFile:        String  = null,
    firstFile:           String  = null,
    secondFile:          String  = null,
    signatureFile:       String  = null,
    dsInitSize:          Int     = 100,
    dsIncreSize:         Int     = 50,
    calibrationPercent:  Double  = 0.3,
    numIterations:       Int     = 50,
    topN:                Int     = 30,
    topPer:              Double  = 3.5,
    bottomPer:           Double  = 0.15,
    singleCycle:         Boolean = false,
    stratified:          Boolean = false,
    confidence:          Double  = 0.2,
    posesCheckpointPath: String  = null,
    size:                String  = "30",
    pdbCode:             String  = null,
    jdbcHostname:        String  = null)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("DockerWithML") {
      head("DockerWithML makes use of Machine learning for efficient Docking")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<conformers-file>")
        .required()
        .text("path to input SDF conformers file")
        .action((x, c) => c.copy(conformersFile = x))
      arg[String]("<sig2Id-file>")
        .required()
        .text("path to save sig2Id")
        .action((x, c) => c.copy(sig2IdPath = x))
      arg[String]("<receptor-file>")
        .required()
        .text("path to input PDBQT receptor file")
        .action((x, c) => c.copy(receptorFile = x))
      arg[String]("<top-poses-path>")
        .required()
        .text("path to top output poses")
        .action((x, c) => c.copy(topPosesPath = x))
      arg[String]("<signature-file>")
        .required()
        .text("path to write and read intermediate signatures")
        .action((x, c) => c.copy(signatureFile = x))
      opt[String]("first-file")
        .text("path to input file with top N mols")
        .action((x, c) => c.copy(firstFile = x))
      opt[String]("second-file")
        .text("path to input file that you want to check for accuracy")
        .action((x, c) => c.copy(secondFile = x))
      opt[String]("posesCheckpointPath")
        .text("path to checkpoint all of the output poses before taking the top 10 (default: null)")
        .action((x, c) => c.copy(posesCheckpointPath = x))
      opt[Int]("dsInitSize")
        .text("initial Data Size to be docked (default: 100)")
        .action((x, c) => c.copy(dsInitSize = x))
      opt[Int]("dsIncreSize")
        .text("incremental Data Size to be docked (default: 50)")
        .action((x, c) => c.copy(dsIncreSize = x))
      opt[Double]("calibrationPercent")
        .text("calibration Percent from training set (default: 0.3)")
        .action((x, c) => c.copy(calibrationPercent = x))
      opt[Int]("numIterations")
        .text("number of iternations for the ML model training (default: 100)")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("topPer")
        .text("percentage taken as topMols from DsInit for training set")
        .action((x, c) => c.copy(topPer = x))
      opt[Double]("bottomPer")
        .text("percentage taken as bottomMols from DsInit for training set")
        .action((x, c) => c.copy(bottomPer = x))
      opt[Int]("topN")
        .text("number of top scoring poses to extract (default: 30).")
        .action((x, c) => c.copy(topN = x))
      opt[Unit]("singleCycle")
        .text("if set the model training will be done only once (for testing purposes)")
        .action((_, c) => c.copy(singleCycle = true))
      opt[Unit]("stratified")
        .text("if set, stratified sampling is performed for calibrationSplit")
        .action((_, c) => c.copy(stratified = true))
      opt[Double]("confidence")
        .text("confidence for conformal prediction (default: 1 - 0.2)")
        .action((x, c) => c.copy(confidence = x))
      opt[String]("size")
        .text("it controls how many molecules are handled within a task (default: 30).")
        .action((x, c) => c.copy(size = x))
      opt[String]("pdbCode")
        .required()
        .text("receptor PDB code")
        .action((x, c) => c.copy(pdbCode = x))
      opt[String]("jdbcHostname")
        .required()
        .text("jdbc hostname")
        .action((x, c) => c.copy(jdbcHostname = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
    System.exit(0)
  }

  def run(params: Arglist) {
    //Init Spark
    val conf = new SparkConf()
      .setAppName("DockerWithML1")

    if (params.master != null) {
      conf.setMaster(params.master)
    }

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.registerKryoClasses(Array(
      classOf[ConformersWithSignsPipeline],
      classOf[scala.collection.immutable.Map$EmptyMap$],
      classOf[org.apache.spark.mllib.regression.LabeledPoint],
      classOf[Array[org.apache.spark.mllib.regression.LabeledPoint]],
      classOf[org.apache.spark.mllib.linalg.SparseVector],
      classOf[org.apache.spark.mllib.linalg.DenseVector],
      classOf[Array[Int]],
      classOf[Array[Double]],
      classOf[Array[String]],
      classOf[scala.collection.mutable.WrappedArray$ofRef]))

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("se.uu.farmbio.parsers.SDFRecordReader.size", params.size)

    val signatures = new SBVSPipeline(sc)
      .readConformerFile(params.conformersFile)
      .generateSignatures(params.sig2IdPath)
      .getMolecules
      .saveAsTextFile(params.signatureFile)

    sc.stop()

    val conf2 = new SparkConf()
      .setAppName("DockerWithML2")
    if (params.master != null) {
      conf2.setMaster(params.master)
    }

    conf2.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf2.set("spark.kryo.registrationRequired", "true")
    conf2.registerKryoClasses(Array(
      classOf[ConformersWithSignsPipeline],
      classOf[scala.collection.immutable.Map$EmptyMap$],
      classOf[org.apache.spark.mllib.regression.LabeledPoint],
      classOf[Array[org.apache.spark.mllib.regression.LabeledPoint]],
      classOf[org.apache.spark.mllib.linalg.SparseVector],
      classOf[org.apache.spark.mllib.linalg.DenseVector],
      classOf[Array[Int]],
      classOf[Array[Double]],
      classOf[Array[String]],
      classOf[scala.collection.mutable.WrappedArray$ofRef],
      classOf[scala.reflect.ClassTag$$anon$1],
      classOf[java.lang.Class[_]]))

    val sc2 = new SparkContext(conf2)
    sc2.hadoopConfiguration.set("se.uu.farmbio.parsers.SDFRecordReader.size", params.size)

    val conformerWithSigns = new SBVSPipeline(sc2)
      .readConformerWithSignsFile(params.signatureFile)
      .dockWithML(
        params.receptorFile,
        params.pdbCode,
        params.jdbcHostname,
        params.dsInitSize,
        params.dsIncreSize,
        params.calibrationPercent,
        params.numIterations,
        params.topPer.toFloat,
        params.bottomPer.toFloat,
        params.singleCycle,
        params.stratified,
        params.confidence)

    val predictedTopPoses = conformerWithSigns.getTopPoses(params.topN)

    sc2.parallelize(predictedTopPoses, 1).saveAsTextFile(params.topPosesPath)

    if (params.firstFile != null && params.secondFile != null ) {

      val mols1 = sc2.hadoopFile[LongWritable, Text, SDFInputFormat](params.firstFile, 2)
        .flatMap(mol => SBVSPipeline.splitSDFmolecules(mol._2.toString))

      val Array1 = mols1.map { mol => PosePipeline.parseId(mol) }.collect()

      val mols2 = sc2.hadoopFile[LongWritable, Text, SDFInputFormat](params.secondFile, 2)
        .flatMap(mol => SBVSPipeline.splitSDFmolecules(mol._2.toString))

      val Array2 = mols2.map { mol => PosePipeline.parseId(mol) }.collect()

      var counter: Double = 0.0
      for (i <- 0 to Array1.length - 1)
        for (j <- 0 to Array2.length - 1)
          if (Array1(i) == Array2(j))
            counter = counter + 1

      logInfo("JOB_INFO: Number of molecules matched are " + counter)
      logInfo("JOB_INFO: Percentage of same results is " + (counter / params.topN) * 100)
    }
    //Reading receptor name from path
    val rName = FilenameUtils.removeExtension(Paths.get(params.receptorFile).getFileName.toString())

    //Saving All molecule scores to Database
    //Getting parameters ready in Row format
    val paramsAsRow = conformerWithSigns.getMolecules
      .map { mol =>
        (rName, PosePipeline.parseIdAndScore(mol))
      }
      .map {
        case (r_name, idAndscore) =>
          Row(r_name, params.pdbCode, idAndscore._1, idAndscore._2)
      }

    //Creating sqlContext Using sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val schema =
      StructType(
        StructField("r_name", StringType, false) ::
          StructField("r_pdbCode", StringType, false) ::
          StructField("l_id", StringType, false) ::
          StructField("l_score", DoubleType, false) :: Nil)

    //Creating DataFrame using row parameters and schema
    val df = sqlContext.createDataFrame(paramsAsRow, schema)

    val prop = new java.util.Properties
    prop.setProperty("driver", "org.mariadb.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "2264421_root")

    //jdbc mysql url - destination database is named "db_profile"
    val url = "jdbc:mysql://" + params.jdbcHostname + ":3306/db_profile"

    //destination database table
    val table = "DOCKED_LIGANDS"

    //write data from spark dataframe to database
    df.write.mode("append").jdbc(url, table, prop)
    logInfo("JOB_INFO: Writing to DOCKED_LIGANDS")
    df.printSchema()

    sc2.stop()

  }

}

