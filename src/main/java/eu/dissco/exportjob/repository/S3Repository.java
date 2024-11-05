package eu.dissco.exportjob.repository;

import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.exceptions.S3UploadException;
import eu.dissco.exportjob.properties.S3Properties;
import java.io.File;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

@Slf4j
@Repository
@RequiredArgsConstructor
public class S3Repository {

  private final S3AsyncClient s3Client;
  private final DateTimeFormatter formatter;
  private final S3Properties properties;

  public String uploadResults(File file, UUID jobId) throws S3UploadException {
    log.info("Uploading results to S3");
    var key = getDate() + "/" + jobId.toString();
    try (var transferManager = S3TransferManager.builder().s3Client(s3Client).build()) {
      var upload = transferManager
          .uploadFile(uploadFileRequest -> uploadFileRequest
              .putObjectRequest(putObjectRequest -> putObjectRequest
                  .bucket(properties.getBucketName())
                  .key(key))
              .source(file));
      upload.completionFuture().join();
      log.info("Successfully uploaded results to S3");
      var url = s3Client.utilities().getUrl(
              builder -> builder
                  .bucket(properties.getBucketName())
                  .key(key))
          .toString();
      s3Client.close();
      return url;
    }
    catch (Exception e){
      log.error("An error has occurred of type {}", e.getClass(), e);
      if (e instanceof SdkClientException ex){
        log.error("Additional information {}", ex.getMessage());
      }
      throw new S3UploadException();
    }
  }

  private String getDate() {
    return formatter.format(Instant.now());
  }

}
