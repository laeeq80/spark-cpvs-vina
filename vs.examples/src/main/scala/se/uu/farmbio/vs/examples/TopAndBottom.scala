package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline
import se.uu.farmbio.vs.PosePipeline
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import java.io.PrintWriter
import org.apache.spark.rdd.RDD
import scala.math.round

object TopAndBottom extends Logging {
  case class Params(
    master:                String = null,
    poseFile:              String = null,
    topAndBottomPosesPath: String = null,
    topPer:                Int    = 0,
    bottomPer:             Int    = 0)

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("TopNBottomPoses") {
      head("TopNBottomPoses: it gets the topPer and bottomPer of poses with smaller shuffle during sorting.")
      opt[String]("master")
        .text("spark master")
        .required
        .action((x, c) => c.copy(master = x))
      opt[Int]("topPer")
        .text("Percentage of top scoring poses to extract.")
        .required()
        .action((x, c) => c.copy(topPer = x))
      opt[Int]("bottomPer")
        .text("Percentage of bottom scoring poses to extract.")
        .required()
        .action((x, c) => c.copy(bottomPer = x))
      arg[String]("<poses-file>")
        .required()
        .text("path to input SDF poses file")
        .action((x, c) => c.copy(poseFile = x))
      arg[String]("<topBottom-poses-path>")
        .required()
        .text("path to top and bottom output poses")
        .action((x, c) => c.copy(topAndBottomPosesPath = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }

  }

  def run(params: Params) {
    //Init Spark
    val conf = new SparkConf()
      .setAppName("TopNBottomPoses")

    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)

    val poses = new SBVSPipeline(sc)
      .readPoseFile(params.poseFile)
      .getMolecules

    val xyz = getTopAndBottom(poses, 19, params.topPer, params.bottomPer)
    /*
    //Get scores with mols
    val molAndScore = poses.map {
      case (mol) => (mol, PosePipeline.parseScore(mol))
    }

    //Map Partitions and sortBy Score
    val topPerPartition = molAndScore.mapPartitions(
      iter => {
        iter.toArray.sortWith((x, y) => x._2.compare(y._2) < 0).take(params.topN).iterator
      },
      preservesPartitioning = true)

    val bottomPerPartition = molAndScore.mapPartitions(
      iter => {
        iter.toArray.sortWith((x, y) => x._2.compare(y._2) < 0).takeRight(params.topN).iterator
      },
      preservesPartitioning = true)

    val topN = topPerPartition.sortBy { case (mol, score) => score }
      .zipWithIndex()
      .filter { case ((mol, score), index) => index < params.topN }
      .map { case ((mol, score), index) => mol }
      .collect()

    val bottomN = bottomPerPartition.sortBy { case (mol, score) => -score }
      .zipWithIndex()
      .filter { x => x._2 < params.topN }
      .map { case ((mol, score), index) => mol }
      .collect()

    //sc.parallelize(sortedPartitions, 1).saveAsTextFile(params.topAndBottomPosesPath)
    val pw = new PrintWriter(params.topAndBottomPosesPath)
    bottomN.foreach(pw.println(_))
    pw.close*/
    sc.stop()

  }

  def getTopAndBottom(poses: RDD[String], dsSize: Int, topPer: Int, bottomPer: Int)  {
    //what is topPer of dsSize
    val topN = round((topPer.toFloat / 100) * dsSize);
    logInfo("JOB_INFO: The value of topPer is " + topPer)
    logInfo("JOB_INFO: The value of topN is " + topN)
    
    val bottomN = round((bottomPer.toFloat / 100) * dsSize);
    logInfo("JOB_INFO: The value of bottomPer is " + bottomPer)
    logInfo(s"JOB_INFO: The value of bottomN is " + bottomN)
    
    //Get scores with mols
    val molAndScore = poses.map {
      case (mol) => (mol, PosePipeline.parseScore(mol))
    }

    //Map Partitions and sortBy Score
    val topPerPartition = molAndScore.mapPartitions(
      iter => {
        iter.toArray.sortWith((x, y) => x._2.compare(y._2) < 0).take(topN).iterator
      },
      preservesPartitioning = true)
      
    val bottomPerPartition = molAndScore.mapPartitions(
      iter => {
        iter.toArray.sortWith((x, y) => x._2.compare(y._2) < 0).takeRight(bottomN).iterator
      },
      preservesPartitioning = true)

    val top = topPerPartition.sortBy { case (mol, score) => score }
      .zipWithIndex()
      .filter { case ((mol, score), index) => index < topN }
      .map { case ((mol, score), index) => mol }
    val collectedTop = top.collect()

    val bottom = bottomPerPartition.sortBy { case (mol, score) => -score }
      .zipWithIndex()
      .filter { x => x._2 < bottomN }
      .map { case ((mol, score), index) => mol }
    val collectedBottom = bottom.collect()
    
    val topAndBottom = top.union(bottom).collect

    val pw = new PrintWriter("data/top.sdf")
    collectedTop.foreach(pw.println(_))
    pw.close

    val pw2 = new PrintWriter("data/bottom.sdf")
    collectedBottom.foreach(pw2.println(_))
    pw2.close
    
    val pw3 = new PrintWriter("data/topAndBottom.sdf")
    topAndBottom.foreach(pw3.println(_))
    pw3.close
    

  }

}