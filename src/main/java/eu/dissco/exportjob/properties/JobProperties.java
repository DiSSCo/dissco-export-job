package eu.dissco.exportjob.properties;

import eu.dissco.exportjob.domain.TargetType;
import jakarta.validation.constraints.NotBlank;
import java.util.List;
import java.util.UUID;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties(prefix = "job")
public class JobProperties {
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

}
