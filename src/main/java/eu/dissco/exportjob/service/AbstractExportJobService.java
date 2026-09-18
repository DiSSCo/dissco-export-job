package eu.dissco.exportjob.service;

import static eu.dissco.exportjob.utils.ExportUtils.removeProxy;

import eu.dissco.exportjob.Profiles;
import eu.dissco.exportjob.component.JobRequestComponent;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.JobStateEndpoint;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.exceptions.S3UploadException;
import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.repository.SourceSystemRepository;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import freemarker.template.TemplateException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

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
  public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(
          "yyyy-MM-dd").withZone(ZoneOffset.UTC);
  public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(
          "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC);
  protected final ElasticSearchRepository elasticSearchRepository;
  protected final IndexProperties indexProperties;
  protected final JsonMapper mapper;
  private final JobRequestComponent jobRequestComponent;
  private final S3Repository s3Repository;
  private final Environment environment;
  public void handleMessage(JobRequest jobRequest) throws FailedProcessingException {
    try {
      jobRequestComponent.updateJobState(jobRequest, JobStateEndpoint.RUNNING);
      var uploadData = processRequest(jobRequest);
      if (uploadData) {
        postProcessResults(jobRequest);
        var url = s3Repository.uploadResults(new File(indexProperties.getTempFileLocation()),
            jobRequest.jobId(), extensionMap.get(environment.getActiveProfiles()[0]));
        log.info("S3 results available at {}", url);
        jobRequestComponent.markAsComplete(jobRequest, url);
      } else {
        log.warn("No results found for job {}", jobRequest.jobId());
        jobRequestComponent.markAsComplete(jobRequest, null);
      }
      log.info("Successfully completed job {}", jobRequest.jobId());
    } catch (IOException | S3UploadException | FailedProcessingException e) {
      log.error("An error has occurred", e);
      jobRequestComponent.updateJobState(jobRequest, JobStateEndpoint.FAILED);
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
        lastId = searchResult.getLast().get(ID_FIELD).asString();
        resultsProcessed += searchResult.size();
      }
    }
    elasticSearchRepository.shutdown();
    log.info("Processed {} search results", resultsProcessed);
    return resultsProcessed > 0;
  }


  protected abstract void writeHeaderToFile() throws IOException;

  protected abstract void postProcessResults(JobRequest jobRequest)
      throws IOException, FailedProcessingException;

  protected abstract void processSearchResults(List<JsonNode> searchResults)
      throws IOException, FailedProcessingException;

  protected abstract List<String> targetFields();

}
