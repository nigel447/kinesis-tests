package aws.kinesis.common

import aws.env.EnvManager
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.regions.Regions
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.streams.asSequence

enum class LOCALSTACK_CONFIG(var param: String) {
    DEFAULT_HOST("localhost"),
    KINESIS_PORT("4568"),
    DYNAMO_PORT("8000"),
    S3_PORT("4572"),
    S3_BUCKET("test.bucket"),
    TEST_ACCESS_ID("LOCALSTACK_ACCESS_ID"),
    TEST_ACCESS_SECRET("LOCALSTACK_SECRET"),
    KINESIS_TOPIC("test"),
    KINESIS_APP_NAME("KinesisClient"),
    KINESIS_DATA_PUSH_MSSG("push this message to s3"),
    KINESIS_DATA_PUSH_MSSG_S3_KEY("test_result")

}

object CredentialsProvider : AWSCredentialsProvider {
    override fun getCredentials(): AWSCredentials {
        return BasicAWSCredentials(LOCALSTACK_CONFIG.TEST_ACCESS_ID.param, LOCALSTACK_CONFIG.TEST_ACCESS_SECRET.param)
    }

    override fun refresh() {

    }
}

object LocalStackConfig {

    // swap for https re localstack or http dynamo lease tables
    fun endpointResolver(isLocalStackHost: Boolean, isS3:Boolean): AwsClientBuilder.EndpointConfiguration {
        if (isLocalStackHost) {
            if(isS3) {
                return AwsClientBuilder.EndpointConfiguration(resolveHostTemplate(LOCALSTACK_CONFIG.DEFAULT_HOST.param,
                    LOCALSTACK_CONFIG.S3_PORT,true), Regions.US_EAST_2.getName())
            } else {
                return AwsClientBuilder.EndpointConfiguration(resolveHostTemplate(LOCALSTACK_CONFIG.DEFAULT_HOST.param,
                    LOCALSTACK_CONFIG.KINESIS_PORT,true), Regions.US_EAST_2.getName())
            }

        } else {
            return AwsClientBuilder.EndpointConfiguration(resolveHostTemplate(
                LOCALSTACK_CONFIG.DEFAULT_HOST.param,
                LOCALSTACK_CONFIG.DYNAMO_PORT, false), Regions.US_EAST_2.getName());
        }
    }

    fun credentials(): AWSCredentialsProvider {
        return CredentialsProvider
    }

    private fun resolveHostTemplate(ip: String, port: LOCALSTACK_CONFIG, isTLS: Boolean): String {
        if(isTLS) {
            return "https://$ip:${port.param}"
        } else {
            return "http://$ip:${port.param}"
        }
    }

    fun bootstrapEnvironment() {

        val envs = hashMapOf<String, String>()
        envs.put("AWS_CBOR_DISABLE", "1")
        envs.put("AWS_ACCESS_KEY_ID", "some_aws_access_key_id")
        envs.put("AWS_SECRET_ACCESS_KEY", "some_aws_secret_access_key")
        EnvManager.INSTANCE.setEnv(envs)

    }


}