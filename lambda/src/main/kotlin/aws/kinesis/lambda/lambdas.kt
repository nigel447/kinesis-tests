package aws.kinesis.lambda

import aws.kinesis.common.Constants
import aws.kinesis.common.LocalStackConfig
import com.amazonaws.services.lambda.AWSLambda
import com.amazonaws.services.lambda.AWSLambdaClientBuilder
import com.amazonaws.services.lambda.model.*
import jlambda.KinesisLambdaHandlerS3
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.zip.ZipEntry
import java.util.zip.ZipFile
import java.util.zip.ZipOutputStream

object LambdaClientProvider {

    fun lambdaClient(): AWSLambda {

        val client = AWSLambdaClientBuilder.standard()
            .withCredentials(LocalStackConfig.credentials())
            .withEndpointConfiguration(LocalStackConfig.endpointResolver(true,false, true )).build()

        return client
    }

}



class LambdaProvider(val streamArn:String) {
    val debug = true
    val projectBasePath = Paths.get("")
    val shaddowJar = projectBasePath.resolve("lambda/build/libs/lambda-1-all.jar")

    val lambda = LambdaClientProvider.lambdaClient()

    var request = CreateFunctionRequest()
    val functionName = "test"

    fun createFunctionCode(): FunctionCode {
        val code = FunctionCode()
        val zf = ZipFile(shaddowJar.toString())
        val zipOut = ByteArrayOutputStream()
        val zipOutStream = ZipOutputStream(zipOut)
        val entries = zf.entries().iterator()
        while (entries.hasNext()) {
            val entry = entries.next()
            val zipEntry = ZipEntry(entry.name)
            zipOutStream.putNextEntry(zipEntry)
            zf.getInputStream(entry).copyTo(zipOutStream, 1024)
            zipOutStream.closeEntry()
        }

        zipOutStream.close()
        zipOut.close()
        val buf = ByteBuffer.wrap(zipOut.toByteArray())
        println("${buf}")
        code.zipFile = buf

        if (debug) {
            val zipFleOut = FileOutputStream(File(".").resolve("test.zip"))
            zipFleOut.write(zipOut.toByteArray())
            zipFleOut.close()
        }

        return code
    }

    fun createToS3EventRequest() {
        request.functionName = functionName
        request.setRuntime(Runtime.Java8)
        request.role = "role"
        request.setCode(createFunctionCode())
        request.setHandler(KinesisLambdaHandlerS3::class.java.getName())

        val funs: ListFunctionsResult = lambda.listFunctions()
        funs.getFunctions().forEach {
            if (it.functionName.equals(functionName)) {
                val delF = DeleteFunctionRequest()
                delF.functionName = functionName
                lambda.deleteFunction(delF)
            }
        }
        lambda.createFunction(request)
    }

    fun mapEventToStream( ) {
        val mapping = CreateEventSourceMappingRequest()
        mapping.functionName = functionName
        mapping.eventSourceArn = streamArn
        mapping.startingPosition = "LATEST"
        val ret = lambda.createEventSourceMapping(mapping)
        Constants.appLogger.info("${ret}")
    }

}


fun main() {

}