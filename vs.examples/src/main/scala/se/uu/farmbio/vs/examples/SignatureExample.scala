package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline
import java.io.PrintWriter

object SignatureExample extends Logging {

  case class Arglist(
    master: String = null,
    conformersFile: String = null,
    sig2IdPath: String = null,
    signatureOutputFile: String = null)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("SignatureExample") {
      head("SignatureExample: a pipeline to generate molecular signatures from conformers.")
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
      arg[String]("<signature-output-file>")
        .required()
        .text("path to output signature file")
        .action((x, c) => c.copy(signatureOutputFile = x))
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
      .setAppName("SignatureExample")

    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)
    val signatures = new SBVSPipeline(sc)
      .readConformerFile(params.conformersFile)
      .generateSignatures(params.sig2IdPath)
      .getMolecules
      .flatMap(SBVSPipeline.splitSDFmolecules)
      .coalesce(8, false)
      .saveAsTextFile(params.signatureOutputFile)

  }

}

