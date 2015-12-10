package com.dvgodoy.spark.benford.image

import java.awt._
import java.awt.color._
import java.awt.image._
import java.io._
import javax.imageio.ImageIO
import com.dvgodoy.spark.benford.util.JobId
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.scalactic._
import scala.math._
import Accumulation._

object SBA {
  case class SBAImageData(width: Int, height: Int, pixels: Array[Int])
  case class SBAData(width: Int, height: Int, wSize: Int, pixels: Array[Double])

  type SBAImageDataMsg = SBAImageData Or Every[ErrorMessage]
  type SBADataMsg = SBAData Or Every[ErrorMessage]
  type SBAEncodedMsg = String Or Every[ErrorMessage]

  def findD1(v: Double): Double = {
    if (v >= 1) floor(v) else findD1(v * 10)
  }

  def bgp(v: Array[Int]): Double = {
    val benf = Array(0.301029995663981, 0.176091259055681, 0.124938736608300, 0.096910013008056, 0.079181246047625, 0.066946789630613, 0.057991946977687, 0.051152522447381, 0.045757490560675)
    val ed = (benf zipWithIndex).map{case (freq, digit) => (digit + 1, freq)}
    val vmax = v.max
    val vmin = v.min
    val bgp = if (vmax == vmin) {
      -300
    } else {
      val vdiff = (vmax - vmin).toDouble
      val count = v.map(pixel => (pixel - vmin)/vdiff).filter(_ > 0.00000001)
        .map(normalized => (findD1(normalized).toInt, 1))
        .groupBy(_._1)
        .map(digits => (digits._1, digits._2.length))
      val total = count.values.sum.toDouble
      val freq = count.map{ case (digit, count) => (digit, count / total)}
      val existingDigits = freq.keys.toSet
      val both = ed.map{case (digit,benf) => (if (existingDigits.contains(digit)) freq(digit) else 0.0, benf)}
      val bvp = sqrt(both.map{case (od, ed) => pow((od - ed), 2) / ed}.sum)
      (1 - bvp) * 100
    }
    bgp
  }

  def grayscale(pixels: Array[Int]): Array[Int] = {
    (0 to pixels.length - 1).map(_ / 3)
      .zip((pixels zip (Stream continually Array(0.299, 0.587, 0.114)).flatten).map{ case (rgb, weight) => rgb * weight })
      .groupBy{ case (idx, rgb) => idx }
      .map{ case (idx, pixel) => (idx, pixel.map{ case (idx, rgb) => rgb }.sum.toInt) }.toArray
      .sortBy{ case (idx, pixel) => idx }
      .map{ case (idx, pixel) => pixel }
  }

  def loadDirect(baos: java.io.ByteArrayOutputStream): SBAImageDataMsg =  {
    try {
      val is = new ByteArrayInputStream(baos.toByteArray)
      val photo1 = ImageIO.read(is)
      var dummy: Array[Int] = null

      val pixels = photo1.getData.getPixels(0, 0, photo1.getWidth, photo1.getHeight, dummy)
      val width = photo1.getWidth
      val height = photo1.getHeight

      val numDataElem = photo1.getData.getNumDataElements
      if (numDataElem == 1) {
        Good(SBAImageData(width, height, pixels))
      } else {
        Good(SBAImageData(width, height, grayscale(pixels)))
      }
    } catch {
      case ex: Exception => Bad(One(s"Error: ${ex.getMessage}"))
    }

  }

  def loadImage(fileName: String): SBAImageDataMsg  = {
    try {
      val photo1 = ImageIO.read(new File(fileName))
      var dummy: Array[Int] = null

      val pixels = photo1.getData.getPixels(0, 0, photo1.getWidth, photo1.getHeight, dummy)
      val width = photo1.getWidth
      val height = photo1.getHeight

      val numDataElem = photo1.getData.getNumDataElements
      if (numDataElem == 1) {
        Good(SBAImageData(width, height, pixels))
      } else {
        Good(SBAImageData(width, height, grayscale(pixels)))
      }
    } catch {
      case ex: Exception => Bad(One(s"Error: ${ex.getMessage}"))
    }
  }

  def performSBA(sc: SparkContext, imageData: SBAImageDataMsg, wSize: Int = 15)(implicit jobId: JobId): SBADataMsg = {
    try {
      withGood(imageData) { (imageData) =>
        val broadWSize = sc.broadcast(wSize)
        val broadWidth = sc.broadcast(imageData.width)
        val broadPixels = sc.broadcast(imageData.pixels)

        val sbaWidth = imageData.width - wSize + 1
        val sbaHeight = imageData.height - wSize + 1

        if (wSize > imageData.width || wSize > imageData.height) throw new IllegalArgumentException("window size cannot exceed image dimensions.")

        val init = (0 to sbaHeight - 1).toArray
        val initRDD = sc.parallelize(init)
        val offsetsRDD = initRDD.flatMap(offsetY => (0 to broadWidth.value - broadWSize.value).map(offsetX => (offsetX, offsetY)))
        val binsRDD = offsetsRDD.map { case (offsetX, offsetY) => (0 to broadWSize.value - 1).toArray
          .map(_ + offsetX)
          .flatMap(single => (0 to broadWSize.value - 1).toArray.map(_ * broadWidth.value + single + offsetY * broadWidth.value))
        }

        val pixelBinsRDD = binsRDD.map(bin => bin map broadPixels.value)
        val bgpBinsRDD = pixelBinsRDD.map(bgp)
        val bgpBins = bgpBinsRDD.collect()

        SBAData(sbaWidth, sbaHeight, wSize, bgpBins)
      }
    } catch {
      case ex: Exception => Bad(One(s"Error: ${ex.getMessage}"))
    }
  }

  def getSBAImage(sbaData: SBADataMsg, threshold: Double = 0.8, whiteBackground: Boolean = true): SBAEncodedMsg = {
    try {
      val thresholdOk = if (threshold <= 1.0) Good(threshold) else Bad(One("Error: Threshold should be less or equal than 1.0."))

      withGood(sbaData, thresholdOk) { (sbaData, threshold) =>
        val ordered = sbaData.pixels.filter(_ > -300.0).sorted
        val pixelThreshold = ordered((ordered.length * (if (threshold == 0.0) 0.8 else threshold) - 1).toInt)

        val filtered = sbaData.pixels.map(value => if (value > pixelThreshold) value else 0)
        val bmin = filtered.min
        val bmax = filtered.max
        val bdiff = bmax - bmin
        val buffer = filtered.map(value => if (value == 0) (if (whiteBackground) -1 else 0) else (128.0 * (value - bmin) / bdiff)).map(_.toByte)

        val cs = ColorSpace.getInstance(ColorSpace.CS_GRAY)
        val cm = new ComponentColorModel(cs, Array(8), false, true, Transparency.OPAQUE, DataBuffer.TYPE_BYTE)
        val sm = cm.createCompatibleSampleModel(sbaData.width, sbaData.height)
        val db = new DataBufferByte(buffer, sbaData.width * sbaData.height)
        val raster = Raster.createWritableRaster(sm, db, null)
        val result = new BufferedImage(cm, raster, false, null)

        val baos = new ByteArrayOutputStream()
        ImageIO.write(result, "png", baos)
        val is = new ByteArrayInputStream(baos.toByteArray)
        val bytes = IOUtils.toByteArray(is)
        val bytes64 = Base64.encodeBase64(bytes)
        val content = new String(bytes64)
        content
      }
    } catch {
      case ex: Exception => Bad(One(s"Error: ${ex.getMessage}"))
    }
  }
}