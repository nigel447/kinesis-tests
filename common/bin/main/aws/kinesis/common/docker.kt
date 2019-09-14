package aws.kinesis.common

import java.io.*
import java.util.concurrent.TimeUnit
import kotlin.streams.asSequence


enum class DOCKER_EXE(var param: String) {
    USR_PATH("/usr/bin/docker"),
    LOCAL_PATH("/usr/local/bin/docker"),
    NO_DOCKER("No Default Docker Location"),
}
enum class DOCKER_CONFIG(var param: String) {

    NETWORK_CMD("network"),
    NETWORK_INSPECT_CMD("inspect"),
    CONTAINER_NAME("containers_default"),
    NETWORK_RANGE("/16")
}

object DockerHandleProvider {

    private val DEFAULT_WAIT_TIME_MINUTES = 1;

    fun setContainerIPAddress(): Pair<String, String> {
        var lstackIp = ""
        var dynamoIp = ""
        val jNode = Constants.mapper.readTree(networkCmd())
        if (jNode[0].get("Containers").size() == 2) {
            jNode[0].get("Containers").forEach {
                if (it.get("Name").toString().contains("localstack")) {
                    lstackIp = it.get("IPv4Address").toString().replace(DOCKER_CONFIG.NETWORK_RANGE.param, "")
                } else if(it.get("Name").toString().contains("dynamo") ){
                    dynamoIp = it.get("IPv4Address").toString().replace(DOCKER_CONFIG.NETWORK_RANGE.param, "")
                }
            }
        }


        return Pair(pruneJsonDirt(lstackIp), pruneJsonDirt(dynamoIp))
    }

    private fun pruneJsonDirt(dirtyString :String):String {
      return  dirtyString.subSequence(1, dirtyString.length-1).toString()
    }

    private fun networkCmd(): String {

        val processCmd = mutableListOf<String>(
            findDocker().param, DOCKER_CONFIG.NETWORK_CMD.param,
            DOCKER_CONFIG.NETWORK_INSPECT_CMD.param,
            DOCKER_CONFIG.CONTAINER_NAME.param
        )
        val process = ProcessBuilder()
            .command(processCmd)
            .start()

        process.waitFor(DEFAULT_WAIT_TIME_MINUTES.toLong(), TimeUnit.MINUTES)
        val ret = process.waitFor()

        if (ret == 1) throw IllegalArgumentException(processError(process))

        return processOutPut(process)

    }

    private fun findDocker(): DOCKER_EXE {
        val ret = DOCKER_EXE.values().asSequence().filter {
            File(it.param).exists()
        }.toList()

        if (ret.isNotEmpty()) {
            return ret.last()
        } else {
            return DOCKER_EXE.NO_DOCKER
        }
    }


    private fun processOutPut(process: Process): String {

        val dataSink = ByteArrayOutputStream()
        process.getInputStream().copyTo(dataSink)

        return String(dataSink.toByteArray())
    }

    private fun processError(process: Process): String {

        val reader = BufferedReader(InputStreamReader(process.errorStream))
        return reader.lines().asSequence().joinToString()
    }
}
