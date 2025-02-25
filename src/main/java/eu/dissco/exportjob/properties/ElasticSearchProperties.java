package eu.dissco.exportjob.properties;


import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("elasticsearch")
public class ElasticSearchProperties {
  @NotBlank
  private String hostname;
  @Positive
  private int port;
  @NotBlank
  private String username;
  @NotBlank
  private String password;

  @NotBlank
  private String digitalSpecimenIndex = "digital-specimen";

  @NotBlank
  private String digitalMediaObjectIndex = "digital-media";

  @NotNull
  private int pageSize = 300;
}
