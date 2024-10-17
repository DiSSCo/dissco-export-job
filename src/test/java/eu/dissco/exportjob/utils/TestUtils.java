package eu.dissco.exportjob.utils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.JobType;
import eu.dissco.exportjob.domain.SearchParam;
import eu.dissco.exportjob.domain.TargetType;
import java.util.List;
import java.util.UUID;

public class TestUtils {

  private TestUtils() {
  }

  public static final UUID JOB_ID = UUID.fromString("cd5c9ee7-23b1-4615-993e-9d56d0720213");
  public static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules()
      .setSerializationInclusion(Include.NON_NULL);

  private static JobRequest givenJobRequest() {
    return new JobRequest(
        JobType.DOI_LIST,
        givenSearchParams(),
        TargetType.DIGITAL_SPECIMEN,
        UUID.randomUUID()
    );
  }

  private static List<SearchParam> givenSearchParams() {
    return List.of(new SearchParam(
        "ods:organisationID", "https://ror.org/03wkt5x30"));
  }


}
