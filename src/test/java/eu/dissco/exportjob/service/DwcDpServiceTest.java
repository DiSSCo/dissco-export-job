package eu.dissco.exportjob.service;

import static eu.dissco.exportjob.utils.TestUtils.DOWNLOAD_LINK;
import static eu.dissco.exportjob.utils.TestUtils.JOB_ID;
import static eu.dissco.exportjob.utils.TestUtils.MAPPER;
import static eu.dissco.exportjob.utils.TestUtils.TEMP_FILE_NAME;
import static eu.dissco.exportjob.utils.TestUtils.givenJobRequest;
import static eu.dissco.exportjob.utils.TestUtils.givenMediaJson;
import static eu.dissco.exportjob.utils.TestUtils.givenSpecimenJson;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.exportjob.domain.JobStateEndpoint;
import eu.dissco.exportjob.properties.DwcDpProperties;
import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.properties.JobProperties;
import eu.dissco.exportjob.repository.DatabaseRepository;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.web.ExporterBackendClient;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DwcDpServiceTest {

  private DwcDpService service;
  @Mock
  private ElasticSearchRepository elasticSearchRepository;
  @Mock
  private ExporterBackendClient exporterBackendClient;
  @Mock
  private S3Repository s3Repository;
  @Mock
  private IndexProperties indexProperties;
  @Mock
  private DatabaseRepository databaseRepository;
  @Mock
  private JobProperties jobProperties;

  @BeforeEach
  void setup() {
    service = new DwcDpService(elasticSearchRepository, exporterBackendClient, s3Repository,
        indexProperties, MAPPER, databaseRepository, jobProperties, new DwcDpProperties());
  }

  @AfterEach
  void tearDown() throws IOException {
    var path = Path.of(TEMP_FILE_NAME);
    if (path.toFile().exists()) {
      Files.delete(path);
    }
  }

  @Test
  void testHandleMessage() throws Exception {
    // Given
    given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
        List.of(givenSpecimenJson()));
    given(elasticSearchRepository.getTargetForMediaList(List.of("https://doi.org/TEST/Y9H-N1L-J7G",
        "https://doi.org/TEST/WVW-SCM-C9Z"))).willReturn(List.of(givenMediaJson()));
    given(jobProperties.getJobId()).willReturn(JOB_ID);
    given(indexProperties.getTempFileLocation()).willReturn(TEMP_FILE_NAME);
    given(s3Repository.uploadResults(any(), eq(JOB_ID))).willReturn(DOWNLOAD_LINK);
    var materialByte = HexFormat.of().parseHex(
        "ACED00057372002865752E64697373636F2E6578706F72746A6F622E646F6D61696E2E44774344504D6174657269616C000000000000000102002F4C00136173736F63696174656453657175656E6365737400124C6A6176612F6C616E672F537472696E673B4C000D636174616C6F674E756D62657271007E00014C000B636F6C6C6563746564427971007E00014C000D636F6C6C65637465644279494471007E00014C000E636F6C6C656374696F6E436F646571007E00014C000C636F6C6C656374696F6E494471007E00014C00136461746147656E6572616C697A6174696F6E7371007E00014C000E646174654964656E74696669656471007E00014C001164657269766174696F6E4576656E74494471007E00014C000E64657269766174696F6E5479706571007E00014C001B6465726976656446726F6D4D6174657269616C456E74697479494471007E00014C000B646973706F736974696F6E71007E00014C00076576656E74494471007E00014C001765766964656E6365466F724F6363757272656E6365494471007E00014C0018686967686572436C617373696669636174696F6E4E616D6571007E00014C0018686967686572436C617373696669636174696F6E52616E6B71007E00014C00186964656E74696669636174696F6E5265666572656E63657371007E00014C00156964656E74696669636174696F6E52656D61726B7371007E00014C00206964656E74696669636174696F6E566572696669636174696F6E53746174757371007E00014C000C6964656E746966696564427971007E00014C000E6964656E7469666965644279494471007E00014C0013696E666F726D6174696F6E5769746868656C6471007E00014C000F696E737469747574696F6E436F646571007E00014C000D696E737469747574696F6E494471007E00014C00186973506172744F664D6174657269616C456E74697479494471007E00014C00106D6174657269616C43617465676F727971007E00014C00106D6174657269616C456E74697479494471007E00014C00156D6174657269616C456E7469747952656D61726B7371007E00014C00126D6174657269616C456E746974795479706571007E00014C00126D6174657269616C5265666572656E63657371007E00014C000E6F626A6563745175616E7469747971007E00014C00126F626A6563745175616E746974795479706571007E00014C00136F74686572436174616C6F674E756D6265727371007E00014C00146F776E6572496E737469747574696F6E436F646571007E00014C00126F776E6572496E737469747574696F6E494471007E00014C000C7072657061726174696F6E7371007E00014C000C7265636F72644E756D62657271007E00014C000E736369656E74696669634E616D6571007E00014C000C7461786F6E466F726D756C6171007E00014C00077461786F6E494471007E00014C00097461786F6E52616E6B71007E00014C000C7461786F6E52656D61726B7371007E00014C00137479706544657369676E6174696F6E5479706571007E00014C000A7479706553746174757371007E00014C000C74797069666965644E616D6571007E00014C0016766572626174696D4964656E74696669636174696F6E71007E00014C000D766572626174696D4C6162656C71007E00017870707070707074004E68747470733A2F2F7777772E676269662E6F72672F6772736369636F6C6C2F636F6C6C656374696F6E2F61666638623834342D623938662D343234652D623962392D39623037383432336439663370707070707074000B2D313839373730343138377070707070707070707074001968747470733A2F2F726F722E6F72672F303430636B32623836707074002068747470733A2F2F646F692E6F72672F544553542F57344B2D5143362D354835707070707070707074000A546973737565202D20317070707070707070707070");
    given(databaseRepository.getRecords(anyString(), eq(0), eq(10000))).willReturn(
        List.of());
    given(databaseRepository.getRecords("temp_table_cd5c9ee7_material", 0, 10000)).willReturn(
        List.of(materialByte));

    // When
    service.handleMessage(givenJobRequest());

    // Then
    then(elasticSearchRepository).should().shutdown();
    then(exporterBackendClient).should().markJobAsComplete(JOB_ID, DOWNLOAD_LINK);
  }

  @Test
  void testHandleMessageException() throws Exception {
    // Given
    given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
        List.of(givenSpecimenJson()));
    given(elasticSearchRepository.getTargetForMediaList(List.of("https://doi.org/TEST/Y9H-N1L-J7G",
        "https://doi.org/TEST/WVW-SCM-C9Z"))).willReturn(List.of(givenMediaJson()));
    given(jobProperties.getJobId()).willReturn(JOB_ID);
    given(indexProperties.getTempFileLocation()).willReturn("///src/test/resources/temp.zip");

    // When
    service.handleMessage(givenJobRequest());

    // Then
    then(elasticSearchRepository).should().shutdown();
    then(exporterBackendClient).should().updateJobState(JOB_ID, JobStateEndpoint.FAILED);
  }

  @Test
  void testCreateTables() {
    // Given
    given(jobProperties.getJobId()).willReturn(JOB_ID);

    // When
    service.setup();

    // Then
    then(databaseRepository).should(times(13)).createTable(anyString());
  }

  @Test
  void testDestroyTables() {
    // Given
    given(jobProperties.getJobId()).willReturn(JOB_ID);

    // When
    service.cleanup();

    // Then
    then(databaseRepository).should(times(13)).dropTable(anyString());
  }
}
