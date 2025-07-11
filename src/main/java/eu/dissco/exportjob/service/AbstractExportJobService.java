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
import eu.dissco.exportjob.repository.SourceSystemRepository;
import eu.dissco.exportjob.web.ExporterBackendClient;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
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
      Profiles.DWC_DP, ".zip",
      Profiles.DWCA, ".zip"
  );
  protected final ElasticSearchRepository elasticSearchRepository;
  protected final IndexProperties indexProperties;
  private final ExporterBackendClient exporterBackendClient;
  private final S3Repository s3Repository;
  private final Environment environment;
  private final SourceSystemRepository sourceSystemRepository;

  public void handleMessage(JobRequest jobRequest) throws FailedProcessingException {
    try {
      exporterBackendClient.updateJobState(jobRequest.jobId(), JobStateEndpoint.RUNNING);
      var uploadData = processRequest(jobRequest);
      if (uploadData) {
        postProcessResults(jobRequest);
        var url = s3Repository.uploadResults(new File(indexProperties.getTempFileLocation()),
            jobRequest.jobId(), extensionMap.get(environment.getActiveProfiles()[0]));
        log.info("S3 results available at {}", url);
        exporterBackendClient.markJobAsComplete(jobRequest.jobId(), url);
      } else {
        log.warn("No results found for job {}", jobRequest.jobId());
        exporterBackendClient.markJobAsComplete(jobRequest.jobId(), null);
      }
      log.info("Successfully completed job {}", jobRequest.jobId());
    } catch (IOException | S3UploadException | FailedProcessingException e) {
      log.error("An error has occurred", e);
      exporterBackendClient.updateJobState(jobRequest.jobId(), JobStateEndpoint.FAILED);
    }
  }

  protected boolean processRequest(JobRequest jobRequest)
      throws IOException, FailedProcessingException {
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

  protected String writeEmlFile(JobRequest jobRequest, FileSystem fs)
      throws FailedProcessingException, IOException {
    var sourceSystemOptional = jobRequest.searchParams().stream()
        .filter(param -> param.inputField().contains("ods:sourceSystemID"))
        .findFirst();
    if (sourceSystemOptional.isEmpty()) {
      throw new FailedProcessingException(
          "Is a source system job, but no sourceSystemID provided: " + jobRequest.jobId());
    }
    var sourceSystemId = sourceSystemOptional.get().inputValue();
    log.info("Retrieving EML for source system ID: {}", sourceSystemId);
    var eml = sourceSystemRepository.getEmlBySourceSystemId(sourceSystemId);
    var sourceSystemFile = fs.getPath("eml.xml");
    Files.writeString(sourceSystemFile, eml, StandardCharsets.UTF_8);
    return eml;
  }

  protected abstract void writeHeaderToFile() throws IOException;

  protected abstract void postProcessResults(JobRequest jobRequest)
      throws IOException, FailedProcessingException;

  protected abstract void processSearchResults(List<JsonNode> searchResults)
      throws IOException, FailedProcessingException;

  protected abstract List<String> targetFields();

}
