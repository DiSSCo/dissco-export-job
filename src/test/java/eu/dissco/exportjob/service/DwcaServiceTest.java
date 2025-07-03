package eu.dissco.exportjob.service;

import static eu.dissco.exportjob.domain.JobStateEndpoint.FAILED;
import static eu.dissco.exportjob.utils.TestUtils.DOWNLOAD_LINK;
import static eu.dissco.exportjob.utils.TestUtils.JOB_ID;
import static eu.dissco.exportjob.utils.TestUtils.MAPPER;
import static eu.dissco.exportjob.utils.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.exportjob.utils.TestUtils.TEMP_FILE_NAME;
import static eu.dissco.exportjob.utils.TestUtils.givenJobRequest;
import static eu.dissco.exportjob.utils.TestUtils.givenMediaJson;
import static eu.dissco.exportjob.utils.TestUtils.givenMinimalSpecimenJson;
import static eu.dissco.exportjob.utils.TestUtils.givenSourceSystemRequest;
import static eu.dissco.exportjob.utils.TestUtils.givenSpecimenJson;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;

import eu.dissco.exportjob.component.DwcaZipWriter;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.exceptions.S3UploadException;
import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.repository.SourceSystemRepository;
import eu.dissco.exportjob.web.ExporterBackendClient;
import freemarker.template.TemplateException;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;

@ExtendWith(MockitoExtension.class)
class DwcaServiceTest {

  private DwcaService service;
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
  @Mock
  private SourceSystemRepository sourceSystemRepository;
  @Mock
  private DwcaZipWriter dwcaZipWriter;

  @BeforeEach
  void setup() {
    service = new DwcaService(elasticSearchRepository, exporterBackendClient, s3Repository,
        indexProperties, MAPPER, environment, sourceSystemRepository, dwcaZipWriter);
  }

  @Test
  void testProcessRecords()
      throws IOException, S3UploadException, FailedProcessingException, TemplateException {
    // Given
    var eml = "<eml></dataset><dataset><title>Test Dataset</title></dataset></eml>";
    given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
        List.of(givenSpecimenJson()));
    given(elasticSearchRepository.getTargetMediaById(List.of("https://doi.org/TEST/Y9H-N1L-J7G",
        "https://doi.org/TEST/WVW-SCM-C9Z"))).willReturn(List.of(givenMediaJson()));
    given(environment.getActiveProfiles()).willReturn(new String[]{"dwca"});
    given(indexProperties.getTempFileLocation()).willReturn(TEMP_FILE_NAME);
    given(s3Repository.uploadResults(any(), eq(JOB_ID), eq(".zip"))).willReturn(DOWNLOAD_LINK);
    given(sourceSystemRepository.getEmlBySourceSystemId(SOURCE_SYSTEM_ID)).willReturn(eml);
    given(dwcaZipWriter.getFileSystem()).willReturn(
        FileSystems.newFileSystem(new File(TEMP_FILE_NAME).toPath(), Map.of("create", "true")));
    // When
    service.handleMessage(givenSourceSystemRequest());

    // Then
    then(dwcaZipWriter).should().writeRecords(anyMap());
    then(elasticSearchRepository).should().shutdown();
    then(exporterBackendClient).should().markJobAsComplete(JOB_ID, DOWNLOAD_LINK);
    then(dwcaZipWriter).should().close();
  }

  @Test
  void testProcessMinimalRecords()
      throws IOException, FailedProcessingException, S3UploadException, TemplateException {
    // Given
    given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
        List.of(givenMinimalSpecimenJson()));
    given(elasticSearchRepository.getTargetMediaById(List.of())).willReturn(List.of());
    given(environment.getActiveProfiles()).willReturn(new String[]{"dwca"});
    given(indexProperties.getTempFileLocation()).willReturn(TEMP_FILE_NAME);
    given(s3Repository.uploadResults(any(), eq(JOB_ID), eq(".zip"))).willReturn(DOWNLOAD_LINK);

    // When
    service.handleMessage(givenJobRequest());

    // Then
    then(dwcaZipWriter).should().writeRecords(anyMap());
    then(elasticSearchRepository).should().shutdown();
    then(exporterBackendClient).should().markJobAsComplete(JOB_ID, DOWNLOAD_LINK);
    then(dwcaZipWriter).should().close();
  }

  @Test
  void testProcessRecordsTemplateException()
      throws IOException, FailedProcessingException, TemplateException {
    // Given
    given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
        List.of(givenMinimalSpecimenJson()));
    given(elasticSearchRepository.getTargetMediaById(List.of())).willReturn(List.of());
    willThrow(new TemplateException(mock(freemarker.core.Environment.class))).given(dwcaZipWriter)
        .close();

    // When
    service.handleMessage(givenJobRequest());

    // Then
    then(dwcaZipWriter).should().writeRecords(anyMap());
    then(elasticSearchRepository).should().shutdown();
    then(exporterBackendClient).should().updateJobState(JOB_ID, FAILED);
  }

}