package eu.dissco.exportjob.service;

import static eu.dissco.exportjob.service.DoiListService.ID_FIELD;

import co.elastic.clients.elasticsearch._types.query_dsl.FieldAndFormat;
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
  protected static final String ID_FIELD = "dcterms:identifier";
  protected static final String PHYSICAL_ID_FIELD = "ods:physicalSpecimenID";


  public void handleMessage(JobRequest jobRequest) throws FailedProcessingException {
    exporterBackendClient.updateJobState(jobRequest.jobId(), JobStateEndpoint.RUNNING);
    try {
      var uploadData = processSearchResults(jobRequest);
      if (uploadData) {
        var url = s3Repository.uploadResults(new File(TEMP_FILE_NAME), jobRequest.jobId());
        exporterBackendClient.markJobAsComplete(jobRequest.jobId(), url);
      } else {
        exporterBackendClient.markJobAsComplete(jobRequest.jobId(), null);
      }
    } catch (IOException e) {
      log.error("An error has occurred", e);
      exporterBackendClient.updateJobState(jobRequest.jobId(), JobStateEndpoint.FAILED);
    }
  }

  private boolean processSearchResults(JobRequest jobRequest) throws IOException {
    String lastId = null;
    writeHeaderToFile();
    boolean keepSearching = true;
    boolean resultsProcessed = false;
    var targetFields = targetFields();
    while (keepSearching) {
      var searchResult = elasticSearchRepository.getTargetObjects(jobRequest.searchParams(),
          jobRequest.targetType(), lastId, targetFields);
      if (searchResult.isEmpty()){
        keepSearching = false;
      } else {
        writeResultsToFile(searchResult);
        lastId = searchResult.getLast().get(ID_FIELD).asText();
        resultsProcessed = true;
      }
    }
    return resultsProcessed;
  }

  protected abstract void writeHeaderToFile() throws IOException;

  protected abstract void writeResultsToFile(List<JsonNode> searchResult) throws IOException;

  protected abstract List<String> targetFields();

}
