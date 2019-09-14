package aws.kinesis.common

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

object HttpCliService {

    var client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(20))
        .followRedirects(HttpClient.Redirect.NEVER)
        .version(HttpClient.Version.HTTP_1_1)
        .build()

    fun pingLocalstack(): Int {
        var response: HttpResponse<String>
        var request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8080"))
            .timeout(Duration.ofSeconds(20))
            .build();

        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString())
            Constants.appLogger.info("localstack ping result  ${response.statusCode()}")
        } catch (e: java.net.ConnectException) {
            Constants.appLogger.info(e.localizedMessage)
            return 1
        }

        return response.statusCode()
    }

}