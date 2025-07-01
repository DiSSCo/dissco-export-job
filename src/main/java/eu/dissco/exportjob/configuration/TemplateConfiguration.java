package eu.dissco.exportjob.configuration;

import static freemarker.template.Configuration.VERSION_2_3_34;

import eu.dissco.exportjob.Profiles;
import freemarker.cache.ClassTemplateLoader;
import freemarker.cache.TemplateLoader;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import org.gbif.dwc.MetaDescriptorWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile({Profiles.DWC_DP, Profiles.DWCA})
@Configuration
@RequiredArgsConstructor
public class TemplateConfiguration {

  private final freemarker.template.Configuration configuration;

  @Bean(name = "dataPackageTemplate")
  public Template dataPackageTemplate() throws IOException {
    return configuration.getTemplate("data-package.ftl");
  }

  @Bean(name = "metaTemplate")
  public Template metaTemplate() throws IOException {
    TemplateLoader tl = new ClassTemplateLoader(MetaDescriptorWriter.class, "/templates");
    var fm = new freemarker.template.Configuration(VERSION_2_3_34);
    fm.setDefaultEncoding("utf8");
    fm.setTemplateLoader(tl);
    fm.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    return fm.getTemplate("meta.ftl");
  }
}
