package aws.kinesis.consumers


import aws.kinesis.common.Constants
import aws.kinesis.common.HttpCliService
import aws.kinesis.common.LOCALSTACK_CONFIG
import aws.kinesis.common.LocalStackConfig
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import java.util.*


object KinesisCosumerClientProvider {

    fun kinesisClient(): AmazonKinesis {
        val builder = AmazonKinesisClientBuilder.standard().withCredentials(LocalStackConfig.credentials())
        builder.withEndpointConfiguration(LocalStackConfig.endpointResolver(true, false) )
        return builder.build()
    }

    fun dynamoDBClient(): AmazonDynamoDB {
        val builder = AmazonDynamoDBClientBuilder.standard().withCredentials(LocalStackConfig.credentials())
        builder.withEndpointConfiguration(LocalStackConfig.endpointResolver(false, false))
        return builder.build()
    }


    fun s3(): AmazonS3 {
        val builder = AmazonS3ClientBuilder.standard()
            .withCredentials(LocalStackConfig.credentials())
            .withEndpointConfiguration(LocalStackConfig.endpointResolver(true,true ))
        builder.isPathStyleAccessEnabled =true
        return builder.build()

    }


    fun cloudWatchMetricsLevel(): MetricsLevel {
        return MetricsLevel.NONE
    }
}


class AwsKinesisClient {
    lateinit var workerThread: Thread
    lateinit var client: AmazonKinesis


    fun bootStrap() {

        val workerId = UUID.randomUUID()
        val config = KinesisClientLibConfiguration(
            LOCALSTACK_CONFIG.KINESIS_APP_NAME.param,
            LOCALSTACK_CONFIG.KINESIS_TOPIC.param,
            LocalStackConfig.credentials(),
            workerId.toString()
        )
            .withMetricsLevel(KinesisCosumerClientProvider.cloudWatchMetricsLevel())
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)

        client = KinesisCosumerClientProvider.kinesisClient()

        val worker = Worker.Builder()
            .recordProcessorFactory(RecordProcessorFactory())
            .config(config)
            .kinesisClient(client)
            .dynamoDBClient(KinesisCosumerClientProvider.dynamoDBClient())
            .build()

        workerThread = Thread(worker, "kinesisListener")

        if (!client.listStreams().streamNames.contains(LOCALSTACK_CONFIG.KINESIS_TOPIC.param)) {
            client.createStream(LOCALSTACK_CONFIG.KINESIS_TOPIC.param, 1)
        }
    }

    fun processStream() {
        workerThread.start()
    }
}

class RecordProcessorFactory : IRecordProcessorFactory {
    override fun createProcessor(): IRecordProcessor {
        return RecordProcessor()
    }
}

class RecordProcessor : IRecordProcessor {

    override fun shutdown(checkpointer: IRecordProcessorCheckpointer?, reason: ShutdownReason?) { }

    override fun initialize(shardId: String?) { }

    override fun processRecords(records: MutableList<Record>?, checkpointer: IRecordProcessorCheckpointer?) {
        records!!.forEach {
            val data = String(it.data.array())
            Constants.appLogger.info(("Processing record pk:${it.partitionKey} -- Data:$data"))
            if(data.contains(LOCALSTACK_CONFIG.KINESIS_DATA_PUSH_MSSG.param)) {
                S3Api.upLoadObject(LOCALSTACK_CONFIG.KINESIS_DATA_PUSH_MSSG.param.toByteArray(),
                    LOCALSTACK_CONFIG.KINESIS_DATA_PUSH_MSSG_S3_KEY.param)
            }

        }
    }
}


fun main() {
    LocalStackConfig.bootstrapEnvironment()
    System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
    // check for localstack service
    do {
        // let localstack boot
        Thread.sleep(4000)
        val code = HttpCliService.pingLocalstack()
        Constants.appLogger.info("checking localstack status is $code")
    } while (code != 200)

    S3Api.createBucket( )
    val client = AwsKinesisClient()
    client.bootStrap()
    client.processStream()

}


