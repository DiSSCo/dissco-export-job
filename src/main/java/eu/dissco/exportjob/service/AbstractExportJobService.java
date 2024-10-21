package eu.dissco.exportjob.service;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.web.ExporterBackendClient;
import java.io.File;
import java.io.IOException;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Service
@Slf4j
@RequiredArgsConstructor
public abstract class AbstractExportJobService {

  private final ElasticSearchRepository elasticSearchRepository;
  private final ExporterBackendClient exporterBackendClient;
  protected static final String TEMP_FILE_NAME = "src/main/resources/tmp.csv.gz";
  private static final String BUCKET_NAME = "dissco-download";


  public void handleMessage(JobRequest jobRequest) throws FailedProcessingException {
    exporterBackendClient.updateJobState(jobRequest.jobId(), "/running");
    try {
      processSearchResults(jobRequest);
    } catch (IOException e) {
      log.error("An error has occurred", e);
      exporterBackendClient.updateJobState(jobRequest.jobId(), "/failed");
    }
    // todo
    //exporterBackendClient.markJobAsComplete(jobRequest.jobId());
  }

  private void processSearchResults(JobRequest jobRequest) throws IOException {
    var totalHits = elasticSearchRepository.getTotalHits(jobRequest.searchParams(),
        jobRequest.targetType());
    int pageNum = 1;
    var hitsProcessed = 0;
    writeHeaderToFile();
    while (hitsProcessed < totalHits) {
      var searchResult = elasticSearchRepository.getTargetObjects(jobRequest.searchParams(),
          jobRequest.targetType(), pageNum);
      writeResultsToFile(searchResult);
      hitsProcessed = hitsProcessed + searchResult.size();
      pageNum = pageNum + 1;
    }
  }

  protected abstract void writeHeaderToFile() throws IOException;

  protected abstract void writeResultsToFile(List<JsonNode> searchResult) throws IOException;

}
