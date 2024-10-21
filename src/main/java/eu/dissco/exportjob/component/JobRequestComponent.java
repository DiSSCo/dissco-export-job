package eu.dissco.exportjob.component;

import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.SearchParam;
import eu.dissco.exportjob.domain.TargetType;
import jakarta.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class JobRequestComponent {

  @NotBlank
  @Value("#{'${job.input-fields}'.split(',')}")
  List<String> inputFields;

  @NotBlank
  @Value("#{'${job.input-values}'.split(',')}")
  List<String> inputValues;

  @NotBlank
  @Value("${job.target-type}")
  TargetType targetType;

  @NotBlank
  @Value("${job.id}")
  UUID jobId;

  public JobRequest getJobRequest() {
    var searchParams = new ArrayList<SearchParam>();
    if (inputFields.size() != inputValues.size()) {
      log.error("Mismatch between input fields and input values for searching");
      throw new IllegalStateException();
    }
    for (int i = 0; i < inputFields.size(); i++) {
      searchParams.add(new SearchParam(inputFields.get(i), inputValues.get(i)));
    }
    log.info("Received {} job request with id {} and {} search parameters", targetType, jobId,
        searchParams);
    return new JobRequest(searchParams, targetType, jobId);
  }

}
