package eu.dissco.exportjob.configuration;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class ApplicationConfiguration {

  @Bean
  public ObjectMapper objectMapper() {
    var mapper = new ObjectMapper().findAndRegisterModules();
    mapper.setSerializationInclusion(Include.NON_NULL);
    return mapper;
  }
}