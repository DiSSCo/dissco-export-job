package eu.dissco.exportjob.service;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.JobStateEndpoint;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.web.ExporterBackendClient;
import java.io.File;
import java.io.IOException;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public abstract class AbstractExportJobService {

  private final ElasticSearchRepository elasticSearchRepository;
  private final ExporterBackendClient exporterBackendClient;
  private final S3Repository s3Repository;
  protected static final String TEMP_FILE_NAME = "src/main/resources/tmp.csv.gz";


  public void handleMessage(JobRequest jobRequest) throws FailedProcessingException {
    exporterBackendClient.updateJobState(jobRequest.jobId(), JobStateEndpoint.RUNNING.getEndpoint());
    try {
      var uploadData = processSearchResults(jobRequest);
      if (uploadData){
        var url = s3Repository.uploadResults(new File(TEMP_FILE_NAME), jobRequest.jobId());
        exporterBackendClient.markJobAsComplete(jobRequest.jobId(), url);
      } else {
        exporterBackendClient.markJobAsComplete(jobRequest.jobId(), null);
      }
    } catch (IOException e) {
      log.error("An error has occurred", e);
      exporterBackendClient.updateJobState(jobRequest.jobId(), JobStateEndpoint.FAILED.getEndpoint());
    }
  }

  private boolean processSearchResults(JobRequest jobRequest) throws IOException {
    var totalHits = elasticSearchRepository.getTotalHits(jobRequest.searchParams(),
        jobRequest.targetType());
    if (totalHits > 0){
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
      return true;
    }
    return false;
  }

  protected abstract void writeHeaderToFile() throws IOException;

  protected abstract void writeResultsToFile(List<JsonNode> searchResult) throws IOException;

}
