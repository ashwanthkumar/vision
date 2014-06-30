package vision.io

import java.io.{ByteArrayInputStream, File}
import java.util

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.archive.format.arc.ARCConstants
import org.archive.io.arc.{ARCWriter, ARCWriterPool, WriterPoolSettingsData}
import vision.utils.VLogger

import scala.collection.JavaConversions._

case class HTMLRecord(url: String, content: String, statusCode: Int, fetchedTimestamp: Long,
                      supervisorIpAddress: String, contentType: String) {

  private def safeContent = Option(StringUtils.trimToNull(content))

  def contentLength = safeContent.map(c => contentWithArcRecordHeader.length).getOrElse(0)

  def contentWithArcRecordHeader = {
    s"HTTP/1.1 $statusCode OK\r\n" +
      s"Content-Type: $contentType\r\n\r\n" + content
  }

  def contentStream = safeContent.map(contentString => new ByteArrayInputStream(contentWithArcRecordHeader.getBytes)).orNull

  def isValidRecord = safeContent.isDefined
}

class IndixArcWriter(arcFileTemplate: String, shouldCompress: Boolean, outputDirectory: File, maxWriterPoolSize: Int = 5, maxWaitTimeForWriterPool: Int = 1000) extends VLogger {

  private val writerPoolSettings = new WriterPoolSettingsData("", arcFileTemplate, ARCConstants.DEFAULT_MAX_ARC_FILE_SIZE, shouldCompress, util.Arrays.asList(outputDirectory), List())
  private val writerPool = new ARCWriterPool(writerPoolSettings, maxWriterPoolSize, maxWaitTimeForWriterPool)
  log.info(s"Opening ArcWriter for output ${outputDirectory.getAbsolutePath}")

  def write(htmlRecord: HTMLRecord) {
    val writer = writerPool.borrowFile().asInstanceOf[ARCWriter]
    if(htmlRecord.isValidRecord) {
      log.info(s"Writing record for ${htmlRecord.url}")
      writer.write(htmlRecord.url, htmlRecord.contentType, htmlRecord.supervisorIpAddress, htmlRecord.fetchedTimestamp, htmlRecord.contentLength, htmlRecord.contentStream)
    } else {
      log.warn(s"Skipping ${htmlRecord.url} as invalid record")
    }

    writerPool.returnFile(writer)
  }

  def close() {
    log.info(s"Closing the ArcWriter for output ${outputDirectory.getAbsolutePath}")
    writerPool.close()
  }
}

/*
  This is to be used only for locally creating ARC files from htmlPages not to be used for production use.
 */
object ArcWriterJob extends App {
  def getContent(file: String) = FileUtils.readFileToString(new File(file))

  val baseHtmlFileLocation = "/home/ashwanth/htmlFiles/"
  val writer = new IndixArcWriter("arcFile_1_${serialno}", false, new File("/home/ashwanth/arcFiles/"))

  (1 to 100000*6).foreach{index =>
    writer.write(
      HTMLRecord(
        url = s"http://www.test-site.com/page-$index",
        content = getContent(baseHtmlFileLocation + "/helloworld.html"),
        statusCode = 200, fetchedTimestamp = System.currentTimeMillis(), supervisorIpAddress = "127.0.0.1",
        contentType = "text/html"
      )
    )
  }
  //val converter: ARCToWARCConverter = new ARCToWARCConverter
  //converter.transform(new File("/home/ashwanth/arcFiles/arcFile_1_00001.arc"), new File("home/ashwanth/arcFiles/htmlPages.warc.gz"))

  writer.close()
}
