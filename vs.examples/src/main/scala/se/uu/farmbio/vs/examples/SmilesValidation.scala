package se.uu.farmbio.vs.examples

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.ObjectInputStream
import java.io.PrintWriter
import java.sql.DriverManager

import org.apache.spark.mllib.regression.LabeledPoint
import org.openscience.cdk.DefaultChemObjectBuilder
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.io.iterator.IteratingSDFReader

import scopt.OptionParser
import se.uu.farmbio.vs.MLlibSVM
import se.uu.farmbio.vs.SGUtils_Serial
import se.uu.it.cp.InductiveClassifier
import se.uu.farmbio.vs.ConformerPipeline

import scala.io.Source

/**
 * @author laeeq
 */

object SmilesValidation {

  case class Arglist(
    master:         String = null,
    smilesFile: String = null,
    sig2IdPath:     String = null,
    sampleFile:     String = null)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("SmilesValidation") {
      head("Smiles Validation BY P-Values")
      arg[String]("<smiles-file>")
        .required()
        .text("path to input smiles file")
        .action((x, c) => c.copy(smilesFile = x))
      arg[String]("<sig2IdMap-file-Path>")
        .required()
        .text("path for loading old sig2Id Mapping File")
        .action((x, c) => c.copy(sig2IdPath = x))
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
    val smilesAndSdf: Array[(String, String)] = smilesArray.map { smiles =>
      (
        smiles,
        ConformerPipeline.pipeString(smiles, List(obabelPath, "-h", "-ismi", "-osdf", "--gen3d")))
    }

    //Getting Array of smiles and IAtomContainer
    val smilesAndIAtomMol: Array[(String, IAtomContainer)] = smilesAndSdf.flatMap {
      case (smiles, sdfmol) =>
        ConformerPipeline.sdfStringToIAtomContainer(sdfmol)
          .map {
            case (iAtomMol) =>
              (smiles, iAtomMol)

          }}
    //Loading oldSig2ID Mapping
    val oldSig2ID = SGUtils_Serial.loadSig2IdMap(params.sig2IdPath)

    //Generate Signature(in vector form) of New Molecule(s)
    val newSigns = SGUtils_Serial.atoms2LP_carryData(smilesAndIAtomMol, oldSig2ID, 1, 3)

    //Load Model
    val svmModel = loadModel()

    //Predict New molecule(s)
    val p_Values = newSigns.map { case (sdfMols, features) => (svmModel.mondrianPv(features.toArray)) }
  
    //Update Predictions to the Prediction Table
    val pw = new PrintWriter("/home/laeeq/Desktop/Backup/Project5/ReceptorsAndInhibitors/Inhibitors_SMILES/1QCF/small.csv")
    p_Values.foreach( vec =>
        pw.write( vec.toString.drop( 7 ).dropRight( 1 ) + "\n" ))
    pw.close

  }

  def loadModel() = {
    //Connection Initialization
    Class.forName("org.mariadb.jdbc.Driver")
    val jdbcUrl = s"jdbc:mysql://localhost:3306/db_profile?user=root&password=2264421_root"
    val connection = DriverManager.getConnection(jdbcUrl)

    //Reading Pre-trained model from Database
    var model: InductiveClassifier[MLlibSVM, LabeledPoint] = null
    if (!(connection.isClosed())) {

      val sqlRead = connection.prepareStatement("SELECT r_model FROM MODELS WHERE r_pdbCode='1QCF'")
      val rs = sqlRead.executeQuery()
      rs.next()

      val modelStream = rs.getObject("r_model").asInstanceOf[Array[Byte]]
      val modelBaip = new ByteArrayInputStream(modelStream)
      val modelOis = new ObjectInputStream(modelBaip)
      model = modelOis.readObject().asInstanceOf[InductiveClassifier[MLlibSVM, LabeledPoint]]

      rs.close
      sqlRead.close
      connection.close()
    } else {
      println("MariaDb Connection is Close")
      System.exit(1)
    }
    model
  }
  
  def readFile(filename: String): Array[String] = {
    val bufferedSource = Source.fromFile(filename)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close
    lines
}

}