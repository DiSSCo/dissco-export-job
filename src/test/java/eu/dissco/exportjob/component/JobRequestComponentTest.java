package eu.dissco.exportjob.component;

import static eu.dissco.exportjob.utils.TestUtils.JOB_ID;
import static eu.dissco.exportjob.utils.TestUtils.ORG_1;
import static eu.dissco.exportjob.utils.TestUtils.ORG_2;
import static eu.dissco.exportjob.utils.TestUtils.ORG_FIELD_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.BDDMockito.then;

import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.JobStateEndpoint;
import eu.dissco.exportjob.domain.SearchParam;
import eu.dissco.exportjob.domain.TargetType;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.properties.JobProperties;
import eu.dissco.exportjob.web.ExporterBackendClient;
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
    jobRequestComponent = new JobRequestComponent(properties, client);
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
  void testHandleMessageInvalidParams() throws FailedProcessingException {
    // Given
    properties.setInputFields(List.of(ORG_FIELD_NAME));
    properties.setJobId(JOB_ID);
    properties.setInputValues(List.of(ORG_1, ORG_2));
    properties.setTargetType(TargetType.DIGITAL_SPECIMEN.getName());

    // When
    assertThrows(FailedProcessingException.class, () -> jobRequestComponent.getJobRequest());

    // Then
    then(client).should().updateJobState(JOB_ID, JobStateEndpoint.FAILED);
  }

}
