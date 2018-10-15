package se.uu.farmbio.vs.examples

import java.io.PrintWriter

import scala.io.Source

import scopt.OptionParser
import se.uu.farmbio.vs.ConformerPipeline

/**
 * @author laeeq
 */

object SmilesToSDF {

  case class Arglist(
    smilesFile: String = null,
    sdfPath:    String = null)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("SmilesToSDF") {
      head("Convert Smiles to SDF")
      arg[String]("<smiles-file>")
        .required()
        .text("path to input smiles file")
        .action((x, c) => c.copy(smilesFile = x))
      arg[String]("<Sdf-file-Path>")
        .required()
        .text("path to save SDF file")
        .action((x, c) => c.copy(sdfPath = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
    System.exit(0)
  }

  def run(params: Arglist) {

    //Use local obabel if OBABEL_HOME is set
    val obabelPath = if (System.getenv("OBABEL_HOME") != null) {
      println("JOB_INFO: using local obabel: " + System.getenv("OBABEL_HOME"))
      System.getenv("OBABEL_HOME")
    } else {
      println("JOB_ERROR: OBABEL_HOME is not set")
      System.exit(1)
      "PATH NOT FOUND"
    }
    //New molecules
    val smilesArray = readFile(params.smilesFile);

    //Convert smi ligand to sdf format using obabel
    val Sdf: Array[(String)] = smilesArray.map { smiles =>
      (
        smiles,
        ConformerPipeline.pipeString(smiles, List(obabelPath, "-h", "-ismi", "-osdf", "--gen3d")))
    }.map { case (smiles, sdf) => sdf }

    val pw = new PrintWriter(params.sdfPath)
    Sdf.foreach(pw.print(_))
    pw.close
    
    println ("SDF Files are now ready")

  }

  def readFile(filename: String): Array[String] = {
    val bufferedSource = Source.fromFile(filename)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close
    lines
  }

}