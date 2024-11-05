package eu.dissco.exportjob.service;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.exportjob.Profiles;
import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.web.ExporterBackendClient;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile(Profiles.DOI_LIST)
public class DoiListService extends AbstractExportJobService {

  private static final byte[] HEADER = (ID_FIELD + "," + PHYSICAL_ID_FIELD).getBytes(
      StandardCharsets.UTF_8);

  public DoiListService(
      ElasticSearchRepository elasticSearchRepository, ExporterBackendClient exporterBackendClient,
      S3Repository s3Repository, IndexProperties indexProperties) {
    super(elasticSearchRepository, exporterBackendClient, s3Repository, indexProperties);
  }

  protected void writeHeaderToFile() throws IOException {
    try (
        var byteOutputStream = new FileOutputStream(indexProperties.getTempFileLocation());
        var gzip = new GZIPOutputStream(byteOutputStream)) {
      gzip.write(HEADER, 0, HEADER.length);
    }
  }

  protected void writeResultsToFile(List<JsonNode> searchResults) throws IOException {
    try (
        var byteOutputStream = new FileOutputStream(indexProperties.getTempFileLocation(), true);
        var gzip = new GZIPOutputStream(byteOutputStream)) {
      for (var result : searchResults) {
        var col = ("\n" + result.get(ID_FIELD).asText() + "," + result.get(PHYSICAL_ID_FIELD).asText())
            .getBytes(StandardCharsets.UTF_8);
        gzip.write(col, 0, col.length);
      }
    }
  }

  protected List<String> targetFields(){
   return List.of(ID_FIELD, PHYSICAL_ID_FIELD);
  }


}
