package eu.dissco.exportjob.configuration;

import eu.dissco.exportjob.Profiles;
import freemarker.template.Template;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile(Profiles.DWC_DP)
@Configuration
@RequiredArgsConstructor
public class TemplateConfiguration {

  private final freemarker.template.Configuration configuration;

  @Bean(name = "dataPackageTemplate")
  public Template dataPackageTemplate() throws IOException {
    return configuration.getTemplate("data-package.ftl");
  }
}
