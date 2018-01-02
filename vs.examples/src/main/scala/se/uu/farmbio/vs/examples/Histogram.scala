package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline
import java.io.PrintWriter
import se.uu.farmbio.vs.PosePipeline


/**
 * @author laeeq
 */

object Histogram extends Logging {

  case class Arglist(
    master: String = null,
    conformersFile: String = null,
    sdfPath: String = null,
    sampleN: Int = 0)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("Take") {
      head("Counts number of molecules in conformer file")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<conformers-file>")
        .required()
        .text("path to input SDF conformers file")
        .action((x, c) => c.copy(conformersFile = x))
      arg[String]("<sdf-Path>")
        .required()
        .text("path to subset SDF file")
        .action((x, c) => c.copy(sdfPath = x))
      opt[Int]("sampleN")
        .required()
        .text("number of mols you want to sample")
        .action((x, c) => c.copy(sampleN = x))  
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
      .setAppName("Take")

    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)

    val mols = new SBVSPipeline(sc)
      .readConformerFile(params.conformersFile)
      .getMolecules
      .flatMap { mol => SBVSPipeline.splitSDFmolecules(mol.toString) }
    val sampleRDD = mols.sample(false, params.sampleN / mols.count.toDouble)
      logInfo("\nJOB_INFO: ################################################################\n")

      logInfo("JOB_INFO: sample size is  " + sampleRDD.count)
      
    val parseScoreRDD = sampleRDD.map(PosePipeline.parseScore)
    val parseScoreHistogram = parseScoreRDD.histogram(10)
    
    parseScoreHistogram._1.foreach(println(_))
    parseScoreHistogram._2.foreach(println(_))


    sc.stop()

  }

}

