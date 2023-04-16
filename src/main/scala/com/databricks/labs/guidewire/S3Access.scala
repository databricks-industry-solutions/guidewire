package com.databricks.labs.guidewire

import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import io.delta.standalone.actions.AddFile
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

object S3Access {
  // New instance everytime given that tasks are executed in parallel
  def build = new S3Access
}

class S3Access() extends Serializable {

  lazy val s3Client: AmazonS3 = build()
  private val logger = LoggerFactory.getLogger(this.getClass)

  def build(): AmazonS3 = {
    AmazonS3ClientBuilder.standard().withForceGlobalBucketAccessEnabled(true).build()
  }

  def listTimestampDirectories(bucketName: String, bucketKey: String): Seq[Long] = {
    logger.info(s"Listing directories in [$bucketKey]")
    val listObjectsRequest = new ListObjectsRequest(bucketName, bucketKey, null, "/", Int.MaxValue)
    val response = s3Client.listObjects(listObjectsRequest)
    val results = response.getCommonPrefixes.asScala.map(_.split("/").last.toLong)
    logger.info(s"Found ${results.size} directory(ies) for schema [$bucketKey]")
    results
  }

  def listParquetFiles(bucketName: String, bucketKey: String): Array[AddFile] = {
    logger.info(s"Listing parquet files in [$bucketKey]")
    val listObjectsRequest = new ListObjectsRequest(bucketName, bucketKey, null, "/", Int.MaxValue)
    val response = s3Client.listObjects(listObjectsRequest)
    val results = response.getObjectSummaries.asScala.filter(file => {
      val fileName = file.getKey.split("/").last
      !fileName.startsWith(".") && fileName.contains(".parquet")
    }).map(summary => {
      val filePath = s"s3://${summary.getBucketName}/${summary.getKey}"
      val jmap = new util.HashMap[String, String]()
      new AddFile(filePath, jmap, summary.getSize, summary.getLastModified.getTime, true, null, jmap)
    }).toArray
    logger.info(s"Found ${results.length} parquet file(s) for schema [$bucketKey]")
    results
  }

  def readString(bucketName: String, bucketKey: String): String = {
    logger.info(s"Reading text from [$bucketKey]")
    s3Client.getObjectAsString(bucketName, bucketKey)
  }

  def readByteArray(bucketName: String, bucketKey: String): Array[Byte] = {
    logger.info(s"Reading byte array from [$bucketKey]")
    IOUtils.toByteArray(s3Client.getObject(bucketName, bucketKey).getObjectContent)
  }

}
