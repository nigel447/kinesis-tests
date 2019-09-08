package jlambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

import java.util.List;

/**
 * this will run with shipped dependency's so keep to a minimum
 * <p>
 * java.lang.ClassNotFoundException: index.Handler
 * ==> fully qualified name of handler is probably wrong
 */

public class KinesisLambdaHandlerS3 implements RequestHandler<KinesisEvent, Void> {

    S3manager s3 = S3manager.INSTANCE;
    static int counter = 0;

    @Override
    public Void handleRequest(KinesisEvent event, Context context) {

        LambdaLogger logger = context.getLogger();
        logger.log("Received " + event.getRecords().size() + " raw Event Records.");


        List<KinesisEvent.KinesisEventRecord> evts = event.getRecords();
        logger.log(String.format("num kinesis events from getRecords %s", evts.size()));

        logger.log(String.format("first r   event.toString %s", event.toString()));
        String ret = handleS3StringUpload( event.toString(), logger);
        logger.log("handleS3StringUpload result " + ret);

        return null;

    }

    public String handleS3StringUpload(String value, LambdaLogger logger) {

        if (logger == null) {
            System.out.println("createBucket ");
        } else logger.log("createBucket ");
        s3.createBucket();

        if (logger == null) {
            System.out.println("upLoad ");
        } else logger.log("upLoad ");
        s3.upLoad(value, "key" + counter++);

        String ret = s3.showResult();
        if (logger == null) {
            System.out.println("showResult " + ret);
        } else logger.log("showResult " + ret);

        return ret;

    }


}
