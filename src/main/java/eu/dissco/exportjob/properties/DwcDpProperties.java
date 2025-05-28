package eu.dissco.exportjob.properties;

import eu.dissco.exportjob.Profiles;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.validation.annotation.Validated;

@Profile(Profiles.DWC_DP)
@Data
@Validated
@ConfigurationProperties(prefix = "dwc-dp")
public class DwcDpProperties {

  @Positive
  private int dbPageSize = 10000;
}
