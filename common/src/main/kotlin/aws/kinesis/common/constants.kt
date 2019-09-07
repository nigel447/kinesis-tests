package aws.kinesis.common

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.commons.logging.LogFactory

object Constants {
    val appLogger = LogFactory.getLog("kinesis-testing")
    val mapper = ObjectMapper().registerModule(KotlinModule())

}