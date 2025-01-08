package eu.dissco.exportjob.service;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.JobStateEndpoint;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.exceptions.S3UploadException;
import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.web.ExporterBackendClient;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
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
  protected final IndexProperties indexProperties;
  protected static final String ID_FIELD = "dcterms:identifier";
  protected static final String PHYSICAL_ID_FIELD = "ods:physicalSpecimenID";


  public void handleMessage(JobRequest jobRequest) throws FailedProcessingException {
    exporterBackendClient.updateJobState(jobRequest.jobId(), JobStateEndpoint.RUNNING);
    try {
      var uploadData = processSearchResults(jobRequest);
      if (uploadData) {
        var url = s3Repository.uploadResults(new File(indexProperties.getTempFileLocation()), jobRequest.jobId());
        log.info("S3 results available at {}", url);
        exporterBackendClient.markJobAsComplete(jobRequest.jobId(), url);
      } else {
        exporterBackendClient.markJobAsComplete(jobRequest.jobId(), null);
      }
      log.info("Successfully completed job {}", jobRequest.jobId());
    } catch (IOException | S3UploadException e) {
      log.error("An error has occurred", e);
      exporterBackendClient.updateJobState(jobRequest.jobId(), JobStateEndpoint.FAILED);
    }
  }

  private boolean processSearchResults(JobRequest jobRequest) throws IOException {
    String lastId = null;
    writeHeaderToFile();
    boolean keepSearching = true;
    long resultsProcessed = 0L;
    var targetFields = targetFields();
    while (keepSearching) {
      var searchResult = elasticSearchRepository.getTargetObjects(jobRequest.searchParams(),
          jobRequest.targetType(), lastId, targetFields);
      if (searchResult.isEmpty()) {
        keepSearching = false;
      } else {
        writeResultsToFile(searchResult);
        lastId = searchResult.getLast().get(ID_FIELD).asText();
        resultsProcessed += searchResult.size();
      }
    }
    elasticSearchRepository.shutdown();
    log.info("Processed {} search results", resultsProcessed);
    return resultsProcessed > 0;
  }

  protected abstract void writeHeaderToFile() throws IOException;

  protected abstract void writeResultsToFile(List<JsonNode> searchResult) throws IOException;

  protected abstract List<String> targetFields();

}
