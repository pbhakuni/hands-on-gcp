package com.example;

// Imports the Google Cloud client library
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.threeten.bp.Duration;

/**
 * The program calls bigquery and updates the data and pushes it back to BQ
 * 
 */
public class DataLoadBQ
{
    public static void main( String[] args ) throws FileNotFoundException, IOException, JobException, InterruptedException   
    {
        System.out.println("Inside main class - Processing begins for bigquery"+ "\n");
        String projectname = args[0];
        String authenticationkey = args[1];
        String datasetName = args[2];
        String tableName = args[3];
        String bucketName = args[4];
        String blobName = args[5];
        String dataFormat = "CSV";
        String destinationUri = "gs://" + bucketName + "/" + blobName;
        String destFilePath  = "C:\\Users\\1029565\\Documents\\GCP Learning\\PubsubandGCS\\furtinure.csv";
        String newFilePath  = "C:\\Users\\1029565\\Documents\\GCP Learning\\PubsubandGCS\\furtinure_latest.csv";
        BigQuery bigquery =  BigQueryOptions.newBuilder().setProjectId(projectname)
                                            .setCredentials(GoogleCredentials
                                            .fromStream(new FileInputStream(authenticationkey)))
                                            .build().getService();
        exportDataToLocal(bigquery, authenticationkey, projectname, datasetName, tableName, destinationUri,dataFormat,bucketName,blobName, destFilePath);
        modifyDataInLocal(destFilePath, newFilePath);
        uploadDataToBQ(bigquery, newFilePath, datasetName, tableName);
    }

    public static void exportDataToLocal(BigQuery bigquery, String authenticationkey, String projectname, String datasetName, String tableName, String destinationUri,String dataFormat, String bucketName, String blobName, String destFilePath) throws JobException, InterruptedException, FileNotFoundException, IOException  
    {

        try {
           
            TableId tableId = TableId.of(projectname, datasetName, tableName);
            Table table = bigquery.getTable(tableId);
            Job job = table.extract(dataFormat, destinationUri);
            // Blocks until this job completes its execution, either failing or succeeding.
            Job completedJob =
                job.waitFor(
                    RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
                    RetryOption.totalTimeout(Duration.ofMinutes(3)));
            
            if (completedJob == null) 
            {
                System.out.println("Job not executed since it no longer exists.");
                return;
            } 
            else if (completedJob.getStatus().getError() != null) 
            {
                System.out.println("BigQuery was unable to extract due to an error: \n" + job.getStatus().getError());
                return;
            }
            System.out.println("Table export successful. Check in GCS bucket for the " + dataFormat + " file.");
        } 
        catch (BigQueryException | InterruptedException e) {
            System.out.println("Table extraction job was interrupted. \n" + e.toString());
        }

        Storage storage = StorageOptions.newBuilder().setProjectId(projectname)
                                        .setCredentials(GoogleCredentials
                                        .fromStream(new FileInputStream(authenticationkey)))
                                        .build().getService();
        Blob blob = storage.get(BlobId.of(bucketName, blobName));
        blob.downloadTo(Paths.get(destFilePath));
        System.out.println(
                "Downloaded object "
                    + blobName
                    + " from bucket name "
                    + bucketName
                    + " to "
                    + destFilePath
                );
        storage.delete(BlobId.of(bucketName, blobName));
    }

    public static void modifyDataInLocal(String destFilePath, String newFilePath) throws IOException 
    {
        List<String> inputLines = Files.readAllLines(Paths.get(destFilePath), StandardCharsets.UTF_8);
        List<String> fixedLines = new ArrayList<>(inputLines.size());

        for(String line: inputLines){
            fixedLines.add(line.replace("Tables & desks", "Workstation"));
        }
        Files.write(
                    Paths.get(newFilePath), 
                    fixedLines,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE
                    );
        System.out.println("File modified and copied to location" + newFilePath);
    }

    public static void uploadDataToBQ(BigQuery bigquery, String filePath, String datasetName, String tableName) throws IOException, InterruptedException 
    {
            System.out.println("Reinserting the data to bigquery - tablename: " + tableName);
            TableId tableId = TableId.of(datasetName, tableName);
             // Skip header row in the file.
            CsvOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build();
            Schema schema = bigquery.getTable(datasetName, tableName).getDefinition().getSchema();
            WriteChannelConfiguration writeChannelConfiguration = WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(csvOptions).setSchema(schema)
                                                                                           .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                                                                                           .build();
            // The location must be specified; other fields can be auto-detected.
            JobId jobId = JobId.newBuilder().setLocation("asia-south1").build();
            TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
            // Write data to writer
            try (OutputStream stream = Channels.newOutputStream(writer)) {
            Files.copy(Paths.get(filePath), stream);
            }
            // Get load job
            Job job = writer.getJob();
            job = job.waitFor();
            LoadStatistics stats = job.getStatistics();
            System.out.println("Total rows inserted to bigquery: " + stats.getOutputRows());
    }
}

