package se.uu.farmbio.vs.examples

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scopt.OptionParser
import se.uu.farmbio.vs.{SBVSPipeline, PosePipeline, ConformersWithSignsPipeline}
import org.apache.hadoop.io.{ LongWritable, Text }
import se.uu.farmbio.parsers.SDFInputFormat

/**
 * @author laeeq
 */

object DockerWithML extends Logging {

  case class Arglist(
    master: String = null,
    posesFileWithSigns: String = null,
    topPosesPath: String = null,
    receptorFile: String = null,
    modelPath: String = null,
    firstFile: String = null,
    secondFile: String = null,
    dsInitSize: Int = 100,
    dsIncreSize: Int = 50,
    calibrationPercent: Double = 0.3,
    numIterations: Int = 50,
    topN: Int = 30,
    badIn: Int = 9,
    goodIn: Int = 6,
    singleCycle: Boolean = false,
    stratified: Boolean = false,
    confidence: Double = 0.2,
    size: String = "30")

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("DockerWithML") {
      head("DockerWithML makes use of Machine learning for efficient Docking")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<conformers-file>")
        .required()
        .text("path to input SDF poses With Signs file")
        .action((x, c) => c.copy(posesFileWithSigns = x))
      arg[String]("<receptor-file>")
        .required()
        .text("path to input PDBQT receptor file")
        .action((x, c) => c.copy(receptorFile = x))
      arg[String]("<top-poses-path>")
        .required()
        .text("path to top output poses")
        .action((x, c) => c.copy(topPosesPath = x))
      
      arg[String]("<model-path>")
        .required()
        .text("path to save model")
        .action((x, c) => c.copy(modelPath = x))  
        
      arg[String]("<first-file>")
        .required()
        .text("path to input file with top N mols")
        .action((x, c) => c.copy(firstFile = x))
      arg[String]("<second-file>")
        .required()
        .text("path to input file that you want to check for accuracy")
        .action((x, c) => c.copy(secondFile = x))
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
      opt[Int]("badIn")
        .text("UpperBound of bad bins")
        .action((x, c) => c.copy(badIn = x))
      opt[Int]("goodIn")
        .text("LowerBound of good bins")
        .action((x, c) => c.copy(goodIn = x))
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
      classOf[Array[se.uu.farmbio.cp.ICPClassifierModel[se.uu.farmbio.cp.UnderlyingAlgorithm]]],
      classOf[se.uu.farmbio.cp.ICPClassifierModelImpl[se.uu.farmbio.cp.UnderlyingAlgorithm]],
      classOf[se.uu.farmbio.cp.alg.SVM],
      classOf[org.apache.spark.mllib.classification.SVMModel],
      classOf[se.uu.farmbio.cp.alg.SVM$$anonfun$$init$$1]  
    ))

    val sc2 = new SparkContext(conf2)
    sc2.hadoopConfiguration.set("se.uu.farmbio.parsers.SDFRecordReader.size", params.size)

    val conformerWithSigns = new SBVSPipeline(sc2)
      .readConformerWithSignsFile(params.posesFileWithSigns)
      .dockWithML(params.receptorFile,
        params.dsInitSize,
        params.dsIncreSize,
        params.calibrationPercent,
        params.numIterations,
        params.badIn,
        params.goodIn,
        params.singleCycle,
        params.stratified,
        params.confidence,
        params.modelPath)
      .getTopPoses(params.topN)

    sc2.parallelize(conformerWithSigns, 1).saveAsTextFile(params.topPosesPath)

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
    logInfo("JOB_INFO: Good bins ranges from 0-" + params.goodIn +
      " and bad bins ranges from " + params.badIn + "-10")
    logInfo("JOB_INFO: Number of molecules matched are " + counter)
    logInfo("JOB_INFO: Percentage of same results is " + (counter / params.topN) * 100)
    sc2.stop()

  }

}

