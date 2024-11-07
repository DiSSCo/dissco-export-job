package eu.dissco.exportjob.properties;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties(prefix = "index")
public class IndexProperties {

  @NotBlank
  String tempFileLocation;

  @NotBlank
  String tempFileLocationZip = tempFileLocation + ".gz";

}
