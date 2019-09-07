package aws.kinesis.consumers

import aws.kinesis.common.Constants
import aws.kinesis.common.LOCALSTACK_CONFIG
import com.amazonaws.services.s3.model.Bucket
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.apache.http.entity.ContentType
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

object S3Api {

    val s3 = KinesisCosumerClientProvider.s3()

    fun createBucket( ): Bucket {
        return  s3.createBucket(LOCALSTACK_CONFIG.S3_BUCKET.param)
    }

   fun upLoadObject(dataBytes: ByteArray, key: String): String {
       val metaData = ObjectMetadata()
       metaData.contentType = ContentType.DEFAULT_TEXT.toString()
       metaData.contentEncoding = StandardCharsets.UTF_8.name()
       metaData.contentLength = dataBytes.size.toLong()

       val putObjectRequest = PutObjectRequest(
           LOCALSTACK_CONFIG.S3_BUCKET.param, key,
           ByteArrayInputStream(dataBytes), metaData)
       val objResult = s3.putObject(putObjectRequest)
       Constants.appLogger.info("upLoadObject etag ${objResult.eTag}}")
       return objResult.eTag
    }

}