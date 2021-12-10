package com.example;

// Imports the Google Cloud client library
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.nio.file.Files;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The program writes the data to gcs bucket and pubusb topics and reads from it
 * 
 */
public class DataLoad
{
    public static void main( String[] args ) throws FileNotFoundException, IOException, InterruptedException, ExecutionException  
    {

        System.out.println("Inside main class - Processing begins"+ "\n");
        String projectname = args[0];
        String bucketname = args[1];
        String filepath = args[2];
        String authenticationkey = args[3];
        String targetBucket = args[4]; // Change this to something unique
        String topicId = args[5];
        String subscriptionname = args[6];
        Storage storage = StorageOptions.newBuilder().setProjectId(projectname)
                                        .setCredentials(GoogleCredentials
                                            .fromStream(new FileInputStream(authenticationkey)))
                                        .build().getService();
        //storage.create(BucketInfo.of(targetBucket));
        uploadDataToGCS(storage, bucketname, filepath);
        readDataFromGCS(storage, bucketname, targetBucket);
        sendDataToPubsub(storage, projectname, bucketname, topicId, authenticationkey);
        readDataFromPubsub(projectname, topicId, subscriptionname, authenticationkey);
        System.out.println("Inside main class - Processed successfully");
    }

    public static void uploadDataToGCS(Storage storage, String bucketname, String filepath) throws FileNotFoundException, IOException {

        //Write data to GCS
        System.out.println("--- Started - Upload processing to GCS ---");
        File[] files = new File(filepath).listFiles();
        for (File file : files) {
            if (file.isFile()) {
                BlobId blobId = BlobId.of(bucketname, file.getName());
                BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                storage.create(blobInfo,  Files.readAllBytes(Paths.get(file.toString())));
            }
        }
        System.out.println("--- Completed - Upload processing to GCS ---"+ "\n");
    }

    public static void readDataFromGCS(Storage storage, String bucketname, String targetbucketname) {
       
        //Read data from GCS
        System.out.println("--- Started - Read processing from GCS ---");
        Page<Blob> b = storage.list(bucketname);

        for ( Blob blob : b.iterateAll()) {
              System.out.println("Name of the object is " + blob);
              blob.copyTo(targetbucketname);
        } 
        System.out.println("--- Completed - Read processing from GCS ---"+ "\n");     
    } 

    public static void sendDataToPubsub(Storage storage, String projectname, String bucketname, String topicId, String authenticationkey) throws IOException, InterruptedException, ExecutionException {
        //Send data from GCS to pubsub
        System.out.println("--- Started - pubsub processing ---");
        Publisher publisher = null;

        try {
            TopicName topicName = TopicName.of(projectname, topicId);
            Page<Blob> b = storage.list(bucketname);
            GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(authenticationkey));
            publisher = Publisher.newBuilder(topicName).setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                                 .build();

            for ( Blob blob : b.iterateAll()) {
                byte[] content = blob.getContent();
                ByteString data = ByteString.copyFrom(content);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                String messageid = messageIdFuture.get();
                System.out.println("Published message id = " + messageid);
            }
        }

        finally {
            if(publisher !=null) {
                  publisher.shutdown();  
            }
        }
        System.out.println("--- Completed - pubsub processing ---"+ "\n");
    }

    public static void readDataFromPubsub(String projectname, String topicname, String subscriptionname, String authenticationkey) throws FileNotFoundException, IOException {

        System.out.println("--- Started - Receiver processing ---" + "\n");
        ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectname, subscriptionname);
        MessageReceiver receiver = 
        (PubsubMessage message, AckReplyConsumer consumer) -> {
            System.out.println("Id: " + message.getMessageId());
            consumer.ack();
        };
        Subscriber subscriber = null;
        
        try {
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(authenticationkey));
        subscriber = Subscriber.newBuilder(subscription, receiver).setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
        // Start the subscriber.
        subscriber.startAsync().awaitRunning();
        // Allow the subscriber to run for 30s unless an unrecoverable error occurs.
        subscriber.awaitTerminated(10, TimeUnit.SECONDS);
        } 
        catch (TimeoutException timeoutException) {
        // Shut down the subscriber after 30s. Stop receiving messages.
        subscriber.stopAsync();
        }

        System.out.println("--- Completed - Receiver processing ---");
    }
}
