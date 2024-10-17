package eu.dissco.exportjob.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.TargetType;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.web.ExporterBackendClient;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class ExportJobService {

  private final ElasticSearchRepository elasticSearchRepository;
  private final ExporterBackendClient exporterBackendClient;
  private static final String ODS_ID = "ods:ID";
  private static final String PHYSICAL_ID = "ods:physicalSpecimenID";

  public void handleMessage(JobRequest jobRequest) {
    exporterBackendClient.updateJobState(jobRequest.jobId(), "/running");
    try {
      var searchResultFile = processSarchResults(jobRequest);
    } catch (IOException e) {
      log.error("An error has occurred", e);
      exporterBackendClient.updateJobState(jobRequest.jobId(), "/failed");
    }
  }


  private File processSarchResults(JobRequest jobRequest) throws IOException {
    var totalHits = elasticSearchRepository.getTotalHits(jobRequest.searchParams(), jobRequest.targetType());
    int pageNum = 1;
    var hitsProcessed = 0;
    var file = writeHeadersToFile(jobRequest);
    while (hitsProcessed < totalHits) {
      var searchResult = elasticSearchRepository.getTargetObjects(jobRequest.searchParams(), jobRequest.targetType(), pageNum);
      writeResultsToFile(searchResult, jobRequest, file);
      hitsProcessed = hitsProcessed + searchResult.size();
      pageNum = pageNum + 1;
    }
    return file;
  }

  private File writeHeadersToFile(JobRequest jobRequest) throws IOException {
    var file = new File("src/main/resources/tmp.csv");
    try (
        var fileWriter = new FileWriter(file);
        var csvWriter = new CSVWriter(fileWriter)) {
      var header = getHeadersFromJobType(jobRequest);
      csvWriter.writeNext(header);
    }
    return file;
  }

  private void writeResultsToFile(List<JsonNode> searchResult, JobRequest jobRequest, File file)
      throws IOException {
    try (
        var fileWriter = new FileWriter(file, true);
        var csvWriter = new CSVWriter(fileWriter)) {
      switch (jobRequest.jobType()) {
        case DOI_LIST -> writeDoiList(searchResult, csvWriter);
      }
    }
  }

  private void writeDoiList(List<JsonNode> searchResults, CSVWriter csvWriter) {
    searchResults.forEach(
        searchResult -> csvWriter.writeNext(
            new String[]{searchResult.get(ODS_ID).asText(),
                searchResult.get(PHYSICAL_ID).asText()})
    );
  }

  private static String[] getHeadersFromJobType(JobRequest jobRequest) {
    switch (jobRequest.jobType()) {
      case DOI_LIST -> {
        return new String[]{ODS_ID, PHYSICAL_ID};
      }
      default -> {
        log.error("Unknown job type {}", jobRequest.jobType());
        throw new UnsupportedOperationException();
      }
    }
  }


}
