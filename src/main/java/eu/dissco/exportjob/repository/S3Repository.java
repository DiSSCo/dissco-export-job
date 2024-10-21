package eu.dissco.exportjob.repository;

import java.io.File;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

@Repository
@RequiredArgsConstructor
public class S3Repository {

  private S3AsyncClient s3Client;
  private S3TransferManager transferManager;
  private static final String BUCKET_NAME = "dissco-download";

  public void upload(File file, UUID jobId) {
    var request = PutObjectRequest.builder()
        .bucket(BUCKET_NAME)
        .key(jobId.toString())
        .build();
    var body = AsyncRequestBody.fromFile(file);
    var upload = transferManager.upload(builder -> builder
        .requestBody(body)
        .putObjectRequest(request)
        .build());


  }

}
