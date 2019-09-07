package aws.kinesis.producer

import aws.kinesis.common.Constants
import aws.kinesis.common.LOCALSTACK_CONFIG
import aws.kinesis.common.LocalStackConfig
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.producer.KinesisProducer
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import com.amazonaws.services.kinesis.producer.UserRecordFailedException
import com.amazonaws.services.kinesis.producer.UserRecordResult
import com.google.common.collect.Iterables
import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures
import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong


/**
 * KPL === Kinesis Producer Library = https://github.com/awslabs/amazon-kinesis-producer/
 */
object KPLProducerClientProvider {

        fun kinesisProducer(): KinesisProducer {
            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");

            val config = KinesisProducerConfiguration()
            config.setRegion(Regions.US_EAST_2.getName())
            config.setCredentialsProvider(LocalStackConfig.credentials())
            config.setMaxConnections(1)
            config.setKinesisEndpoint(LOCALSTACK_CONFIG.DEFAULT_HOST.param)
            config.setKinesisPort(LOCALSTACK_CONFIG.KINESIS_PORT.param.toLong())
            config.setRequestTimeout(60000)
            config.setRecordMaxBufferedTime(15000)
            config.setVerifyCertificate(false)

            return KinesisProducer(config)
        }
}


object TestProducer {
    val countDownLatch = CountDownLatch(1)
    val counter = AtomicLong()
    val producer = KPLProducerClientProvider.kinesisProducer()

    fun processor(testCount:Int) {

        (1..testCount).forEach {
            if(it == 5) {
                val f =  producer.addUserRecord(
                    LOCALSTACK_CONFIG.KINESIS_TOPIC.param,
                    "1", ByteBuffer.wrap(LOCALSTACK_CONFIG.KINESIS_DATA_PUSH_MSSG.param.toByteArray())
                )
                Futures.addCallback(f, callBackFactory())
            } else {
                val f =  producer.addUserRecord(
                    LOCALSTACK_CONFIG.KINESIS_TOPIC.param,
                    "1", ByteBuffer.wrap("this is test $it".toByteArray())
                )
                Futures.addCallback(f, callBackFactory())
            }

        }
        producer.flush()
        countDownLatch.await()
    }

    fun callBackFactory(): FutureCallback<UserRecordResult> {

        return object:FutureCallback<UserRecordResult> {
            override fun onSuccess(result: UserRecordResult?) {
                Constants.appLogger.info("UserRecord Success - shardId:${ result!!.shardId }, message count:${counter.incrementAndGet()}" )

                if(counter.get().compareTo(10) == 0) {
                    Constants.appLogger.info("message count = 10 => terminate" )
                    countDownLatch.countDown()
                }
            }

            override fun onFailure(t: Throwable) {
                if (t is UserRecordFailedException) {
                 val last =   Iterables.getLast(t.result.attempts)
                   Constants.appLogger.info("Record failed to put - ${ last.getErrorCode()}: ${ last.getErrorMessage()}" )
                }
            }
        }
    }

}


fun main() {
      TestProducer.processor(10)

}