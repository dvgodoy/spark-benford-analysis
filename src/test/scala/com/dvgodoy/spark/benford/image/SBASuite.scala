package com.dvgodoy.spark.benford.image

import java.io.File
import javax.imageio.ImageIO

import com.dvgodoy.spark.benford.integration.SparkSuite
import com.dvgodoy.spark.benford.image.SBA._
import com.dvgodoy.spark.benford.util._
import java.security.MessageDigest

class SBASuite extends SparkSuite {
  protected var image: SBAImageDataMsg = _
  protected var sba: SBADataMsg = _
  protected var newImage: SBAEncodedMsg = _
  protected var pixels: Array[Int] = _

  implicit val jobId = JobId("test")

  override def loadAndProcessData(): Unit = {
    val fileName = "./src/test/resources/chess.png"
    val file = new File(fileName)
    val photo1 = ImageIO.read(file)
    var dummy: Array[Int] = null
    pixels = photo1.getData.getPixels(0, 0, photo1.getWidth, photo1.getHeight, dummy)

    image = loadImage(fileName)
    sba = performSBA(sc, image, 3)
    newImage = getSBAImage(sba, 0.8, true)
  }

  def calcHash(param: String): String = {
    MessageDigest.getInstance("MD5").digest(param.getBytes)
      .map(v => Integer.toHexString(0xff & v))
      .map(v => if (v.length == 2) v else '0' + v)
      .mkString
  }

  test("loadImage works") {
    calcHash(image.get.originalImage) must be ("8ae3e026770b732c7a266598acfe4632")
  }

  test("SBA calculation is correct") {
    calcHash(sba.get.pixels.map(_.toString).mkString) must be ("ce8d70d90b1dd0a486ee286f5cda5e29")
  }

  test("new image generation is correct") {
    calcHash(newImage.get) must be ("b998b1cfce285da8d4d389ffb9bb8d93")
  }

  test("bgp works") {
    bgp(pixels) must be (3.169246910344836 +- 1e-10)
  }

  test("grayscale works") {
    calcHash(grayscale(pixels).map(_.toString).mkString) must be ("22e4d0c33978ced6b11c2d69b816bda7")
  }
}
