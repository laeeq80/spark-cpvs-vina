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
import scala.math.round

trait ConformersWithSignsTransforms {
  def dockWithML(
    receptorPath:       String,
    pdbCode:            String,
    jdbcHostname:       String,
    dsInitSize:         Int,
    dsIncreSize:        Int,
    calibrationPercent: Double,
    numIterations:      Int,
    topPer:             Float,
    bottomPer:          Float,
    singleCycle:        Boolean,
    stratified:         Boolean,
    confidence:         Double): SBVSPipeline with PoseTransforms
}

object ConformersWithSignsPipeline extends Serializable with Logging {

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

  private def getFeatureVector(poses: String) = {
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

  private def labelPose(sdfRecord: String, label: Double): String = {
    val it = SBVSPipeline.CDKInit(sdfRecord)
    val strWriter = new StringWriter()
    val writer = new SDFWriter(strWriter)
    while (it.hasNext()) {
      val mol = it.next
      mol.removeProperty("cdk:Remark")
      mol.setProperty("Label", label)
      writer.write(mol)
    }
    writer.close
    strWriter.toString() //return the molecule

  }

  private def getLabeledTopAndBottom(poses: RDD[String], dsSize: Int, topPer: Float, bottomPer: Float): RDD[String] = {
    //what is top % of dsSize
    val topN = round((topPer / 100) * dsSize)
    logInfo("JOB_INFO: topN is " + topN)

    //what is bottom % of dsSize
    val bottomN = round((bottomPer / 100) * dsSize)
    logInfo("JOB_INFO: bottomN is " + bottomN)

    //Get scores with mols
    val molAndScore = poses.map {
      case (mol) => (mol, PosePipeline.parseScore(mol))
    }

    //Sort whole DsInit by Score
    val top = molAndScore.sortBy { case (mol, score) => score }
      .zipWithIndex()
      .filter { case ((mol, score), index) => index < topN }
      .map { case ((mol, score), index) => mol }

    val bottom = molAndScore.sortBy { case (mol, score) => -score }
      .zipWithIndex()
      .filter { x => x._2 < bottomN }
      .map { case ((mol, score), index) => mol }

    //Labeling the top molecules with 1.0
    val labledTop = top.map { topMols =>
      labelPose(topMols, 1.0)
    }
    //Labeling the bottom molecules with 0.0
    val labledBottom = bottom.map { bottomMols =>
      labelPose(bottomMols, 0.0)
    }

    val topAndBottom = labledTop.union(labledBottom)

    topAndBottom
  }

  @deprecated
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
    topPer:             Float,
    bottomPer:          Float,
    singleCycle:        Boolean,
    stratified:         Boolean,
    confidence:         Double) = {

    //initializations
    var poses: RDD[String] = null
    var dsTrain: RDD[String] = null
    var dsOnePredicted: RDD[(String)] = null
    var ds: RDD[String] = rdd.flatMap(SBVSPipeline.splitSDFmolecules).persist(StorageLevel.DISK_ONLY)
    var eff: Double = 0.0
    var counter: Int = 1
    var effCounter: Int = 0
    var calibrationSizeDynamic: Int = 0
    var dsInit: RDD[String] = null
    var dsBadInTrainingSet: RDD[String] = null
    var dsGoodInTrainingSet: RDD[String] = null
    var dsTopAndBottom: RDD[String] = null

    //Converting complete dataset (dsComplete) to feature vector required for conformal prediction
    //We also need to keep intact the poses so at the end we know
    //which molecules are predicted as bad and remove them from main set

    val fvDsComplete = ds.flatMap {
      sdfmol =>
        ConformersWithSignsPipeline.getFeatureVector(sdfmol)
          .map { case (vector) => (sdfmol, vector) }
    }

    do {
      //Step 1
      //Get a sample of the data
      if (dsInit == null)
        dsInit = ds.sample(false, dsInitSize / ds.count.toDouble)
      else
        dsInit = ds.sample(false, dsIncreSize / ds.count.toDouble)

      logInfo("JOB_INFO: Sample taken for docking in cycle " + counter)

      val dsInitToDock = dsInit.mapPartitions(x => Seq(x.mkString("\n")).iterator)

      //Step 2
      //Docking the sampled dataset
      val dsDock = ConformerPipeline
        .getDockingRDD(receptorPath, dockTimePerMol = false, sc, dsInitToDock, true)
        .map {
          case (dirtyMol) => ConformerPipeline.cleanPoses(dirtyMol, true)
        }
        .flatMap(SBVSPipeline.splitSDFmolecules).persist(StorageLevel.DISK_ONLY)

      logInfo("JOB_INFO: Docking Completed in cycle " + counter)

      //Step 3
      //Subtract the sampled molecules from main dataset
      ds = ds.subtract(dsInit)

      //Step 4
      //Keeping processed poses
      if (poses == null) {
        poses = dsDock
      } else {
        poses = poses.union(dsDock)
      }

      //Step 5 and 6 Computing dsTopAndBottom and label it
      if (dsTopAndBottom == null) {
        dsTopAndBottom = ConformersWithSignsPipeline.getLabeledTopAndBottom(dsDock, dsInitSize, topPer, bottomPer)
      } else {
        dsTopAndBottom = ConformersWithSignsPipeline.getLabeledTopAndBottom(dsDock, dsIncreSize, topPer, bottomPer)
      }

      logInfo("JOB_INFO: dsTopAndBottom in cycle " + counter + " is " + dsTopAndBottom.count)

      //Step 7 Union dsTrain and dsTopAndBottom
      if (dsTrain == null) {
        dsTrain = dsTopAndBottom
      } else {
        dsTrain = dsTrain.union(dsTopAndBottom).persist(StorageLevel.DISK_ONLY)
      }

      logInfo("JOB_INFO: Training set size in cycle " + counter + " is " + dsTrain.count)

      //Counting zeroes and ones in each training set in each cycle
      if (dsTrain == null) {
        dsBadInTrainingSet = dsTopAndBottom.filter {
          case (mol) => ConformersWithSignsPipeline.getLabel(mol) == "0.0"
        }
      } else {
        dsBadInTrainingSet = dsTrain.filter {
          case (mol) => ConformersWithSignsPipeline.getLabel(mol) == "0.0"
        }
      }

      if (dsTrain == null) {
        dsGoodInTrainingSet = dsTopAndBottom.filter {
          case (mol) => ConformersWithSignsPipeline.getLabel(mol) == "1.0"
        }
      } else {
        dsGoodInTrainingSet = dsTrain.filter {
          case (mol) => ConformersWithSignsPipeline.getLabel(mol) == "1.0"
        }
      }
      logInfo("JOB_INFO: Zero Labeled Mols in Training set in cycle " + counter + " are " + dsBadInTrainingSet.count)
      logInfo("JOB_INFO: One Labeled Mols in Training set in cycle " + counter + " are " + dsGoodInTrainingSet.count)

      //Converting SDF training set to LabeledPoint(label+sign) required for conformal prediction
      val lpDsTrain = dsTrain.flatMap {
        sdfmol => ConformersWithSignsPipeline.getLPRDD(sdfmol)
      }

      //Step 8 Training
      //Splitting data into Proper training set and calibration set
      val Array(properTraining, calibration) = lpDsTrain.randomSplit(Array(1 - calibrationPercent, calibrationPercent), seed = 11L)

      //Train ICP
      val svm = new MLlibSVM(properTraining.persist(StorageLevel.MEMORY_AND_DISK_SER), numIterations)
      //SVM based ICP Classifier (our model)
      val icp = ICP.trainClassifier(svm, nOfClasses = 2, calibration.collect)

      lpDsTrain.unpersist()
      properTraining.unpersist()

      logInfo("JOB_INFO: Training Completed in cycle " + counter)

      //Step 9 Prediction using our model on complete dataset
      val predictions = fvDsComplete.map {
        case (sdfmol, predictionData) => (sdfmol, icp.predict(predictionData.toArray, confidence))
      }

      val dsZeroPredicted: RDD[(String)] = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(0.0)) }
        .map { case (sdfmol, prediction) => sdfmol }

      //Step 10 Subtracting {0} mols from main dataset
      ds = ds.subtract(dsZeroPredicted).persist(StorageLevel.MEMORY_AND_DISK_SER)

      //Computing efficiency for stopping loop
      val totalCount = sc.accumulator(0.0)
      val singletonCount = sc.accumulator(0.0)

      predictions.foreach {
        case (sdfmol, prediction) =>
          if (prediction.size == 1) {
            singletonCount += 1.0
          }
          totalCount += 1.0
      }

      eff = singletonCount.value / totalCount.value
      logInfo("JOB_INFO: Efficiency in cycle " + counter + " is " + eff)

      if (eff >= 0.8) {
        effCounter = effCounter + 1
      } else {
        effCounter = 0
      }
      counter = counter + 1
      if (effCounter >= 2) {
        dsOnePredicted = predictions
          .filter { case (sdfmol, prediction) => (prediction == Set(1.0)) }
          .map { case (sdfmol, prediction) => sdfmol }
        ConformersWithSignsPipeline.insertModels(receptorPath, icp, pdbCode, jdbcHostname)
        ConformersWithSignsPipeline.insertPredictions(receptorPath, pdbCode, jdbcHostname, predictions, sc)
      }
    } while (effCounter < 2 && !singleCycle)

    if (dsOnePredicted != null) {
      dsOnePredicted = dsOnePredicted.subtract(poses)

      val dsOnePredictedToDock = dsOnePredicted.mapPartitions(x => Seq(x.mkString("\n")).iterator)

      val dsDockOne = ConformerPipeline.getDockingRDD(receptorPath, false, sc, dsOnePredictedToDock, true)
        .map {
          case (dirtyMol) => ConformerPipeline.cleanPoses(dirtyMol, true)
        }
        .flatMap(SBVSPipeline.splitSDFmolecules)

      logInfo("JOB_INFO: dsDockOne in is " + dsDockOne.count)
      //Keeping rest of processed poses i.e. dsOne mol poses
      if (poses == null)
        poses = dsDockOne
      else
        poses = poses.union(dsDockOne)
    }
    new PosePipeline(poses)
  }

}