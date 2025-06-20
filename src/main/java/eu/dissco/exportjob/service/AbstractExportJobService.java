package eu.dissco.exportjob.service;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.exportjob.Profiles;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.JobStateEndpoint;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.exceptions.S3UploadException;
import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.web.ExporterBackendClient;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public abstract class AbstractExportJobService {

  protected static final String ID_FIELD = "dcterms:identifier";
  protected static final String PHYSICAL_ID_FIELD = "ods:physicalSpecimenID";
  private static final Map<String, String> extensionMap = Map.of(
      Profiles.DOI_LIST, ".csv.gz",
      Profiles.DWC_DP, ".zip"
  );
  protected final ElasticSearchRepository elasticSearchRepository;
  protected final IndexProperties indexProperties;
  private final ExporterBackendClient exporterBackendClient;
  private final S3Repository s3Repository;
  private final Environment environment;

  public void handleMessage(JobRequest jobRequest) throws FailedProcessingException {
    try {
      exporterBackendClient.updateJobState(jobRequest.jobId(), JobStateEndpoint.RUNNING);
      var uploadData = processRequest(jobRequest);
      postProcessResults(jobRequest);
      if (uploadData) {
        var url = s3Repository.uploadResults(new File(indexProperties.getTempFileLocation()),
            jobRequest.jobId(), extensionMap.get(environment.getActiveProfiles()[0]));
        log.info("S3 results available at {}", url);
        exporterBackendClient.markJobAsComplete(jobRequest.jobId(), url);
      } else {
        exporterBackendClient.markJobAsComplete(jobRequest.jobId(), null);
      }
      log.info("Successfully completed job {}", jobRequest.jobId());
    } catch (IOException | S3UploadException | FailedProcessingException e) {
      log.error("An error has occurred", e);
      exporterBackendClient.updateJobState(jobRequest.jobId(), JobStateEndpoint.FAILED);
    }
  }

  protected boolean processRequest(JobRequest jobRequest) throws IOException {
    String lastId = null;
    writeHeaderToFile();
    boolean keepSearching = true;
    long resultsProcessed = 0L;
    var targetFields = targetFields();
    while (keepSearching) {
      log.info("Paginating over elastic, resultsProcessed: {}", resultsProcessed);
      var searchResult = elasticSearchRepository.getTargetObjects(jobRequest.searchParams(),
          jobRequest.targetType(), lastId, targetFields);
      if (searchResult.isEmpty()) {
        keepSearching = false;
      } else {
        processSearchResults(searchResult);
        lastId = searchResult.getLast().get(ID_FIELD).asText();
        resultsProcessed += searchResult.size();
      }
    }
    elasticSearchRepository.shutdown();
    log.info("Processed {} search results", resultsProcessed);
    return resultsProcessed > 0;
  }

  protected abstract void writeHeaderToFile() throws IOException;

  protected abstract void postProcessResults(JobRequest jobRequest) throws IOException, FailedProcessingException;

  protected abstract void processSearchResults(List<JsonNode> searchResults) throws IOException;

  protected abstract void writeResultsToFile(List<JsonNode> searchResults) throws IOException;

  protected abstract List<String> targetFields();

}
