package vision.io

import java.io.{ByteArrayOutputStream, BufferedOutputStream, FileOutputStream, File}
import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.archive.format.warc.WARCConstants
import org.archive.format.warc.WARCConstants.WARCRecordType
import org.archive.io.ArchiveRecord
import org.archive.io.arc.{ARCConstants, ARCRecord, ARCReader, ARCReaderFactory}
import org.archive.io.warc.{WARCWriterPoolSettings, WARCRecordInfo, WARCWriterPoolSettingsData, WARCWriter}
import org.archive.uid.UUIDGenerator
import org.archive.util.anvl.ANVLRecord
import org.joda.time.DateTimeZone
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormat}

class ARCToWARCConverter() {
  val generator = new UUIDGenerator()

  def transform(arcFile : File, warcFile : File) = {
    val arcReader = ARCReaderFactory.get(arcFile, false, 0)
    arcReader.setDigest(false)
    val bos = new BufferedOutputStream(new FileOutputStream(warcFile))

    val iter : util.Iterator[ArchiveRecord] = arcReader.iterator()
    val firstRecord: ARCRecord = iter.next.asInstanceOf[ARCRecord]
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream(firstRecord.getHeader.getLength.asInstanceOf[Int])
    firstRecord.dump(baos)

    val ar : ANVLRecord = new ANVLRecord()
    ar.addLabelValue("Filedesc", baos.toString())
    val warcWriter = new WARCWriter(new AtomicInteger, bos, warcFile, new WARCWriterPoolSettingsData("", "", -1, arcReader.isCompressed, null, null, generator))

    while(iter.hasNext) {
      write(warcWriter, iter.next.asInstanceOf[ARCRecord])
    }

    warcWriter.close()
    arcReader.close()
  }

  def createWarcRecordInfo(arcRecord : ARCRecord, anvlRecord : ANVLRecord) : WARCRecordInfo = {
    val recordInfo: WARCRecordInfo = new WARCRecordInfo
    recordInfo.setUrl(arcRecord.getHeader.getUrl)
    recordInfo.setContentStream(arcRecord)
    recordInfo.setContentLength(arcRecord.getHeader.getLength)
    recordInfo.setEnforceLength(true)
    recordInfo.setType(WARCRecordType.response)
    recordInfo.setMimetype(WARCConstants.HTTP_RESPONSE_MIMETYPE)
    recordInfo.setRecordId(generator.getRecordID)
    recordInfo.setExtraHeaders(anvlRecord)
    recordInfo
  }

  def setDate(arcRecord : ARCRecord, recordInfo : WARCRecordInfo) = {
    //convert ARC date to WARC-Date format
    val arcDateString: String = arcRecord.getHeader.getDate()
    val warcDateString: String = DateTimeFormat.forPattern("yyyyMMddHHmmss").withZone(DateTimeZone.UTC).parseDateTime(arcDateString).toString(ISODateTimeFormat.dateTimeNoMillis)
    recordInfo.setCreate14DigitDate(warcDateString)
  }

  def createANVLRecord(arcRecord : ARCRecord) : ANVLRecord = {
     val anvlRecord : ANVLRecord = new ANVLRecord
     val ip: String = arcRecord.getHeader.getHeaderValue("ip-address").asInstanceOf[String]
     anvlRecord.addLabelValue(WARCConstants.NAMED_FIELD_IP_LABEL, ip)
     arcRecord.getMetaData
     anvlRecord
  }

  def write(writer : WARCWriter, arcRecord : ARCRecord) = {
    val anvlRecord: ANVLRecord = createANVLRecord(arcRecord)
    val recordInfo : WARCRecordInfo = createWarcRecordInfo(arcRecord, anvlRecord)
    setDate(arcRecord, recordInfo)
    writer.writeRecord(recordInfo)
  }

}
