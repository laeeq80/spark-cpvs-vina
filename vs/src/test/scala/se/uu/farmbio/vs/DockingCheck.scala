//if you want to compare that serial and spark version gives same result, use this 
//or copy only the test part to SBVSPipelineTest.scala
/*
package se.uu.farmbio.vs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import openeye.oedocking.OEDockMethod
import openeye.oedocking.OESearchResolution
import se.uu.farmbio.parsers.PDBRecordReader

@RunWith(classOf[JUnitRunner])
class DockingCheck extends FunSuite with BeforeAndAfterAll {

  //Init Spark
  private val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("DockingCheck")
  private val sc = new SparkContext(conf)
  sc.hadoopConfiguration.set(PDBRecordReader.SIZE_PROPERTY_NAME, "3")

  test("Docking of 1000 molecules both in Parallel and serial should be same") {

    //Parallel Execution
    val resPar = new SBVSPipeline(sc)
      .readConformerFile(getClass.getResource("chembl_1000_samples.sdf").getPath)
      .dock(getClass.getResource("receptor.oeb").getPath)
      .getMolecules
      .collect

    //Serial Execution   
    val conformerFile = TestUtils.readSDF(getClass.getResource("chembl_1000_samples.sdf").getPath)
    val receptorFileName = getClass.getResource("receptor.oeb").getPath
    //Source for dockingstdSerial can be found in the docking-cpp folder
    val dockingstdFileName = getClass.getResource("dockingstdSerial").getPath
    val conformerStr = conformerFile.mkString("\n")
    val resSer =
      ConformerPipeline.pipeString(conformerStr,
        List(dockingstdFileName,
          receptorFileName))

    assert(resPar.map(TestUtils.removeSDFheader).toSet
      === SBVSPipeline.splitSDFmolecules(resSer).map(TestUtils.removeSDFheader).toSet)
  }

  override def afterAll() {
    sc.stop()
  }

}*/