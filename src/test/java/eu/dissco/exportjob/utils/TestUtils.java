package eu.dissco.exportjob.utils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.SearchParam;
import eu.dissco.exportjob.domain.TargetType;
import java.util.List;
import java.util.UUID;

public class TestUtils {

  private TestUtils() {
  }

  public static final String DOI_1 = "https://doi.org/10.2055/123-456-789";
  public static final String DOI_2 = "https://doi.org/10.2055/ABC-EFG-HIJ";
  public static final String ORG_1 = "https://ror.org/0566bfb96";
  public static final String ORG_2 = "https://ror.org/040ck2b86";
  public static final String PHYS_ID_1 = "AVES.XYZ";
  public static final String PHYS_ID_2 = "AVES.QRS";
  public static final UUID JOB_ID = UUID.fromString("cd5c9ee7-23b1-4615-993e-9d56d0720213");
  public static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules()
      .setSerializationInclusion(Include.NON_NULL);

  private static JobRequest givenJobRequest() {
    return new JobRequest(
        givenSearchParams(),
        TargetType.DIGITAL_SPECIMEN,
        UUID.randomUUID()
    );
  }

  public static List<SearchParam> givenSearchParams() {
    return List.of(new SearchParam(
        "$['ods:organisationID']", ORG_1));
  }

  public static JsonNode givenDigitalSpecimen(){
    return givenDigitalSpecimen(DOI_1, ORG_1, PHYS_ID_1);
  }

  public static JsonNode givenDigitalSpecimen(String doi, String org, String physId){
    return MAPPER.createObjectNode()
        .put("ods:ID", doi)
        .put("@id", doi)
        .put("ods:organisationID", org)
        .put("ods:physicalSpecimenID", physId);
  }
}
