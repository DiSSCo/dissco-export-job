package eu.dissco.exportjob.component;

import static eu.dissco.exportjob.utils.TestUtils.DOWNLOAD_LINK;
import static eu.dissco.exportjob.utils.TestUtils.JOB_ID;
import static eu.dissco.exportjob.utils.TestUtils.JSON_MAPPER;
import static eu.dissco.exportjob.utils.TestUtils.MAPPER;
import static eu.dissco.exportjob.utils.TestUtils.ORG_1;
import static eu.dissco.exportjob.utils.TestUtils.ORG_2;
import static eu.dissco.exportjob.utils.TestUtils.ORG_FIELD_NAME;
import static eu.dissco.exportjob.utils.TestUtils.givenJobRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.BDDMockito.then;

import eu.dissco.exportjob.client.ExporterBackendClient;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.JobStateEndpoint;
import eu.dissco.exportjob.domain.SearchParam;
import eu.dissco.exportjob.domain.TargetType;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.properties.JobProperties;
import eu.dissco.exportjob.utils.TestUtils;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobRequestComponentTest {

  private JobRequestComponent jobRequestComponent;
  private JobProperties properties;
  @Mock
  private ExporterBackendClient client;

  @BeforeEach
  void init(){
    properties = new JobProperties();
    jobRequestComponent = new JobRequestComponent(properties, client, JSON_MAPPER);
  }

  @Test
  void testHandleMessage() throws FailedProcessingException {
    // Given
    var expected = new JobRequest(
        List.of(new SearchParam(ORG_FIELD_NAME, ORG_1), new SearchParam(ORG_FIELD_NAME, ORG_2)),
        TargetType.DIGITAL_SPECIMEN,
        JOB_ID,
        Boolean.FALSE
    );
    properties.setInputFields(List.of(ORG_FIELD_NAME, ORG_FIELD_NAME));
    properties.setJobId(JOB_ID);
    properties.setInputValues(List.of(ORG_1, ORG_2));
    properties.setTargetType(TargetType.DIGITAL_SPECIMEN.getName());
    properties.setIsSourceSystemJob(Boolean.FALSE);

    // When
    var result = jobRequestComponent.getJobRequest();

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testHandleMessageInvalidParams() {
    // Given
    properties.setInputFields(List.of(ORG_FIELD_NAME));
    properties.setJobId(JOB_ID);
    properties.setInputValues(List.of(ORG_1, ORG_2));
    properties.setTargetType(TargetType.DIGITAL_SPECIMEN.getName());

    // When
    assertThrows(FailedProcessingException.class, () -> jobRequestComponent.getJobRequest());

    // Then
    then(client).should().updateJobState(JOB_ID.toString(), JobStateEndpoint.FAILED.getEndpoint());
  }

  @Test
  void testMarkAsComplete() {
    // Given
    var expected = JSON_MAPPER.createObjectNode()
        .put("id", JOB_ID.toString())
        .put("downloadLink", DOWNLOAD_LINK);

    // When
    jobRequestComponent.markAsComplete(givenJobRequest(), DOWNLOAD_LINK);

    // Then
    then(client).should().markJobAsComplete(expected);
  }

  @Test
  void testUpdateJobState() {
    // Given
    var jobRequest = givenJobRequest();

    // When
    jobRequestComponent.updateJobState(jobRequest, JobStateEndpoint.FAILED);

    // Then
    then(client).should().updateJobState(JOB_ID.toString(), JobStateEndpoint.FAILED.getEndpoint());
  }

}
