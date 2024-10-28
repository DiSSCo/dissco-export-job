package eu.dissco.exportjob.repository;

import java.io.File;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

@Repository
@RequiredArgsConstructor
public class S3Repository {

  private final S3AsyncClient s3Client;
  private final DateTimeFormatter formatter;
  private static final String BUCKET_NAME = "dissco-data-export";

  public String uploadResults(File file, UUID jobId) {
    try (var transferManager = S3TransferManager.builder().s3Client(s3Client).build()) {
      var upload = transferManager
          .uploadFile(uploadFileRequest -> uploadFileRequest
              .putObjectRequest(putObjectRequest -> putObjectRequest
                  .bucket(BUCKET_NAME)
                  .key(getDate() + "/" + jobId.toString()))
              .source(file));
      upload.completionFuture().join();
      return s3Client.utilities().getUrl(
              builder -> builder
                  .bucket(BUCKET_NAME)
                  .key(jobId.toString()))
          .toString();
    }
  }

  private String getDate() {
    return formatter.format(Instant.now());
  }

}
