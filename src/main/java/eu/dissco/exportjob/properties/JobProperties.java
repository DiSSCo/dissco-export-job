package eu.dissco.exportjob.properties;

import jakarta.validation.constraints.NotNull;
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

  @NotNull
  @Value("#{'${job.input-fields}'.split(',')}")
  List<String> inputFields;

  @NotNull
  @Value("#{'${job.input-values}'.split(',')}")
  List<String> inputValues;

  @NotNull
  String targetType;

  @NotNull
  UUID jobId;

  @NotNull
  Boolean isSourceSystemJob;

}
