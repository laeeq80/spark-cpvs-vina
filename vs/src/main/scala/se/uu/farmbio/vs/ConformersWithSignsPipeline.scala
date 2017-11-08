package se.uu.farmbio.vs

import se.uu.farmbio.cp.ICP
import se.uu.farmbio.cp.alg.SVM
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.openscience.cdk.io.SDFWriter
import java.io.{StringWriter, PrintWriter}
import org.apache.spark.storage.StorageLevel
import se.uu.farmbio.cp.ICPClassifierModel

trait ConformersWithSignsTransforms {
  def dockWithML(
    receptorPath: String,
    dsInitSize: Int,
    dsIncreSize: Int,
    calibrationPercent: Double,
    numIterations: Int,
    badIn: Int,
    goodIn: Int,
    singleCycle: Boolean,
    stratified: Boolean,
    confidence: Double,
    modelPath: String): SBVSPipeline with PoseTransforms

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

  private def labelTopAndBottom(
    pdbqtRecord: String,
    score: Double,
    scoreHistogram: Array[Double],
    badIn: Int,
    goodIn: Int) = {
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

}

private[vs] class ConformersWithSignsPipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsTransforms {

  override def dockWithML(
    receptorPath: String,
    dsInitSize: Int,
    dsIncreSize: Int,
    calibrationPercent: Double,
    numIterations: Int,
    badIn: Int,
    goodIn: Int,
    singleCycle: Boolean,
    stratified: Boolean,
    confidence: Double,
    modelPath: String) = {

    //initializations
    var poses: RDD[String] = null
    var dsTrain: RDD[String] = null
    var dsOnePredicted: RDD[(String)] = null
    var dsZeroRemoved: RDD[(String)] = null
    var cumulativeZeroRemoved: RDD[(String)] = null
    var ds: RDD[String] = rdd.flatMap(SBVSPipeline.splitSDFmolecules).persist(StorageLevel.MEMORY_AND_DISK_SER)
    var eff: Double = 0.0
    var counter: Int = 1
    var effCounter: Int = 0
    var calibrationSizeDynamic: Int = 0
    var dsInit: RDD[String] = null
    var dsBadInTrainingSet: RDD[String] = null
    var dsGoodInTrainingSet: RDD[String] = null

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
      //Mocking the sampled dataset. We already have scores, docking not required
      val dsDock = dsInit
      logInfo("\nJOB_INFO: cycle " + counter
        + "   ################################################################\n")

      logInfo("JOB_INFO: dsInit in cycle " + counter + " is " + dsInit.count)

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

      //Step 5 and 6 Computing dsTopAndBottom
      val parseScoreRDD = dsDock.map(PosePipeline.parseScore).persist(StorageLevel.MEMORY_ONLY)
      val parseScoreHistogram = parseScoreRDD.histogram(10)

      val dsTopAndBottom = dsDock.map {
        case (mol) =>
          val score = PosePipeline.parseScore(mol)
          ConformersWithSignsPipeline.labelTopAndBottom(mol, score, parseScoreHistogram._1, badIn, goodIn)
      }.map(_.trim).filter(_.nonEmpty)

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
      //Train icps
      calibrationSizeDynamic = (dsTrain.count * calibrationPercent).toInt
      val (calibration, properTraining) = ICP.calibrationSplit(
        lpDsTrain, calibrationSizeDynamic, stratified)

      //Train ICP
      val svm = new SVM(properTraining.persist(StorageLevel.MEMORY_AND_DISK_SER), numIterations)
      //SVM based ICP Classifier (our model)
      val icp = ICP.trainClassifier(svm, numClasses = 2, calibration)
      
      //Saving Models
      sc.parallelize(Seq(icp), 1).saveAsObjectFile(modelPath + counter)
      logInfo("JOB_INFO: Model saved in cycle " + counter + " at " + modelPath + counter)
      
      //Loading Model for testing purposes
      val loadedIcpModel = sc.objectFile[ICPClassifierModel[SVM]](modelPath + counter).first()
      
      parseScoreRDD.unpersist()
      lpDsTrain.unpersist()
      properTraining.unpersist()

      logInfo("JOB_INFO: Training Completed in cycle " + counter)

      //Step 9 Prediction using our model on complete dataset
      val predictions = fvDsComplete.map {
        case (sdfmol, predictionData) => (sdfmol, loadedIcpModel.predict(predictionData, confidence))
      }

      val dsZeroPredicted: RDD[(String)] = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(0.0)) }
        .map { case (sdfmol, prediction) => sdfmol }.cache
      dsOnePredicted = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(1.0)) }
        .map { case (sdfmol, prediction) => sdfmol }.cache
      val dsBothUnknown: RDD[(String)] = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(0.0, 1.0)) }
        .map { case (sdfmol, prediction) => sdfmol }
      val dsEmptyUnknown: RDD[(String)] = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set()) }
        .map { case (sdfmol, prediction) => sdfmol }  
        
      //Step 10 Subtracting {0} moles from dataset which has not been previously subtracted
      if (dsZeroRemoved == null)
        dsZeroRemoved = dsZeroPredicted.subtract(poses)
      else
        dsZeroRemoved = dsZeroPredicted.subtract(cumulativeZeroRemoved.union(poses))

      ds = ds.subtract(dsZeroRemoved)

      logInfo("JOB_INFO: Number of bad mols predicted in cycle " +
        counter + " are " + dsZeroPredicted.count)
      logInfo("JOB_INFO: Number of bad mols removed in cycle " +
        counter + " are " + dsZeroRemoved.count)
      logInfo("JOB_INFO: Number of good mols predicted in cycle " +
        counter + " are " + dsOnePredicted.count)
      logInfo("JOB_INFO: Number of Both Unknown mols predicted in cycle " +
        counter + " are " + dsBothUnknown.count)
      logInfo("JOB_INFO: Number of Empty Unknown mols predicted in cycle " +
        counter + " are " + dsEmptyUnknown.count)
        
      //Keeping all previous removed bad mols
      if (cumulativeZeroRemoved == null)
        cumulativeZeroRemoved = dsZeroRemoved
      else
        cumulativeZeroRemoved = cumulativeZeroRemoved.union(dsZeroRemoved)

      dsZeroPredicted.unpersist()

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
     
    } while (effCounter < 2 && !singleCycle)

    logInfo("JOB_INFO: Total number of bad mols removed are " + cumulativeZeroRemoved.count)

    //Docking rest of the dsOne mols
    val dsDockOne = dsOnePredicted.subtract(poses).cache()
    logInfo("JOB_INFO: Number of mols in dsDockOne are " + dsDockOne.count)

    //Keeping rest of processed poses i.e. dsOne mol poses
    if (poses == null)
      poses = dsDockOne
    else
      poses = poses.union(dsDockOne)

    logInfo("JOB_INFO: Total number of docked mols are " + poses.count)
    new PosePipeline(poses)
  }

}