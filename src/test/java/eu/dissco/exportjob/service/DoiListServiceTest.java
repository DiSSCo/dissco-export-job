package eu.dissco.exportjob.service;

import static eu.dissco.exportjob.Profiles.DOI_LIST;
import static eu.dissco.exportjob.utils.TestUtils.DOWNLOAD_LINK;
import static eu.dissco.exportjob.utils.TestUtils.JOB_ID;
import static eu.dissco.exportjob.utils.TestUtils.TEMP_FILE_NAME;
import static eu.dissco.exportjob.utils.TestUtils.givenDigitalSpecimen;
import static eu.dissco.exportjob.utils.TestUtils.givenJobRequest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.web.ExporterBackendClient;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;

@ExtendWith(MockitoExtension.class)
@ActiveProfiles(profiles = DOI_LIST)
class DoiListServiceTest {

  private DoiListService service;

  @Mock
  private ElasticSearchRepository elasticSearchRepository;
  @Mock
  private ExporterBackendClient exporterBackendClient;
  @Mock
  private S3Repository s3Repository;
  @Mock
  private IndexProperties indexProperties;
  @Mock
  private Environment environment;

  @BeforeEach
  void init() {
    service = new DoiListService(elasticSearchRepository, exporterBackendClient, s3Repository, indexProperties, environment);
  }

  @Test
  void testHandleMessageNoResultsFound() throws Exception {
    // Given
    given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
        List.of());
    given(indexProperties.getTempFileLocation()).willReturn(TEMP_FILE_NAME);

    // When
    service.handleMessage(givenJobRequest());

    // Then
    then(elasticSearchRepository).should().shutdown();
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    then(s3Repository).shouldHaveNoInteractions();
    then(exporterBackendClient).should().markJobAsComplete(JOB_ID, null);
  }

  @Test
  void testHandleMessage() throws Exception {
    // Given
    given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
        List.of(givenDigitalSpecimen()));
    given(environment.getActiveProfiles()).willReturn(new String[]{"doi_list"});
    given(s3Repository.uploadResults(any(), eq(JOB_ID), eq(".csv.gz"))).willReturn(DOWNLOAD_LINK);
    given(indexProperties.getTempFileLocation()).willReturn(TEMP_FILE_NAME);

    // When
    service.handleMessage(givenJobRequest());

    // Then
    then(elasticSearchRepository).should().shutdown();
    then(exporterBackendClient).should().markJobAsComplete(JOB_ID, DOWNLOAD_LINK);
  }

}