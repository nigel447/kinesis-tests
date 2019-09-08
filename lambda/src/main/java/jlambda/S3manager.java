package jlambda;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * running in lambda need to keep everything as local to this package as possible
 *
 * localstack code
 */
public enum S3manager {

    INSTANCE;
    String S3_ENDPOINT_KEY = "lstack_host_name";
    String s3endpointTemplate = "https://%s:4572";
    String DEFAULT_REGION = "us-east-2";
    String TEST_ACCESS_KEY = "AWS_ACCESS_KEY_ID";
    String TEST_SECRET_KEY = "AWS_SECRET_KEY";
    String bucketName = "bucket11";

    AWSCredentials TEST_CREDENTIALS = new BasicAWSCredentials(TEST_ACCESS_KEY, TEST_SECRET_KEY);
    AmazonS3 s3Client;

    S3manager() {
        s3Client = getS3Client();
    }

    AmazonS3 getS3Client() {
        Map<String, String> map = System.getenv();
        String lstack_host =  map.get(S3_ENDPOINT_KEY);
        if(lstack_host == null ){
            lstack_host = "localhost" ;
        }
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");

        String s3endpoint = String.format(s3endpointTemplate,lstack_host);
        AwsClientBuilder.EndpointConfiguration  endPoint =
                new AwsClientBuilder.EndpointConfiguration(s3endpoint, DEFAULT_REGION);
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard().
                withEndpointConfiguration(endPoint).
                withCredentials(new AWSStaticCredentialsProvider(TEST_CREDENTIALS));
        builder.setPathStyleAccessEnabled(true);
        return builder.build();
    }

    void createBucket() {
        if(!s3Client.doesBucketExistV2(bucketName)) {
            s3Client.createBucket(bucketName);
        }
    }

    void upLoad(String data, String key) {
        try {
            File file = Files.createTempFile(key, ".json").toFile();
            file.deleteOnExit();
            ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(file));
            zipOutputStream.putNextEntry(new ZipEntry(file.getName()));
            zipOutputStream.write(data.getBytes());
            zipOutputStream.closeEntry();
            zipOutputStream.close();
            s3Client.putObject(bucketName, file.getName() + ".zip", file);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    String showResult( ) {

        ObjectListing list = s3Client.listObjects(bucketName);
        String result =  list.getObjectSummaries().stream()
                .map(s -> s.toString())
                .collect(Collectors.joining());
        return result;
    }

}
