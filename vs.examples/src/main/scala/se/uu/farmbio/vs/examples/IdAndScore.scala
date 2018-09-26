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

object IdAndScore extends Logging {

  case class Arglist(
    master: String = null,
    conformersFile: String = null,
    outputPath: String = null)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("IdAndScore") {
      head("Get the id And Scores")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<conformers-file>")
        .required()
        .text("path to input SDF conformers file")
        .action((x, c) => c.copy(conformersFile = x))
      arg[String]("<outputPath>")
        .required()
        .text("path to subset SDF file")
        .action((x, c) => c.copy(outputPath = x))
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
      val idAndScore = mols.map {
      case (mol) => PosePipeline.parseIdAndScore(mol)
    }.saveAsTextFile(params.outputPath)
  
/*
    val pw = new PrintWriter(params.outputPath)
    mols.foreach(pw.println(_))
    pw.close
*/
    sc.stop()

  }

}

