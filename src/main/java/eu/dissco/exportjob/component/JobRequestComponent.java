package eu.dissco.exportjob.component;

import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.JobStateEndpoint;
import eu.dissco.exportjob.domain.SearchParam;
import eu.dissco.exportjob.domain.TargetType;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.properties.JobProperties;
import eu.dissco.exportjob.web.ExporterBackendClient;
import java.util.ArrayList;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class JobRequestComponent {

  private final JobProperties properties;
  private final ExporterBackendClient client;

  public JobRequest getJobRequest() throws FailedProcessingException {
    var searchParams = new ArrayList<SearchParam>();
    if (properties.getInputFields().size() != properties.getInputValues().size()) {
      log.error("Mismatch between input fields and input values for searching");
      client.updateJobState(properties.getJobId(), JobStateEndpoint.FAILED);
      throw new FailedProcessingException();
    }
    for (int i = 0; i < properties.getInputFields().size(); i++) {
      searchParams.add(new SearchParam(properties.getInputFields().get(i), properties.getInputValues().get(i)));
    }
    log.info("Received job request with id {} and {} search parameters", properties.getJobId(), searchParams);
    return new JobRequest(searchParams, TargetType.fromString(properties.getTargetType()), properties.getJobId(), properties.getIsSourceSystemJob());
  }

}
