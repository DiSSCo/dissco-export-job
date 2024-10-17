package eu.dissco.exportjob.service;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.exportjob.Profiles;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
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

  private static final String ODS_ID = "ods:ID";
  private static final String PHYSICAL_ID = "ods:physicalSpecimenID";
  private static final byte[] HEADER = (ODS_ID + "," + PHYSICAL_ID).getBytes(
      StandardCharsets.UTF_8);

  public DoiListService(
      ElasticSearchRepository elasticSearchRepository,
      ExporterBackendClient exporterBackendClient) {
    super(elasticSearchRepository, exporterBackendClient);
  }

  protected void writeHeaderToFile() throws IOException {
    try (
        var byteOutputStream = new FileOutputStream(TEMP_FILE_NAME, true);
        var gzip = new GZIPOutputStream(byteOutputStream)) {
      gzip.write(HEADER, 0, HEADER.length);
    }
  }

  protected void writeResultsToFile(List<JsonNode> searchResults) throws IOException {
    try (
        var byteOutputStream = new FileOutputStream(TEMP_FILE_NAME, true);
        var gzip = new GZIPOutputStream(byteOutputStream)) {
      for (var result : searchResults) {
        var col = ("\n" + result.get(ODS_ID).asText() + "," + result.get(PHYSICAL_ID).asText())
            .getBytes(StandardCharsets.UTF_8);
        gzip.write(col, 0, col.length);
      }
    }
  }
}
