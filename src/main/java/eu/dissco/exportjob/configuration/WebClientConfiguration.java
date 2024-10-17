package eu.dissco.exportjob.configuration;

import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;


@Configuration
@RequiredArgsConstructor
public class WebClientConfiguration {

  @NotBlank
  @Value("${backend.endpoint}")
  private String backendEndpoint;

  @Bean(name="exporter")
  public WebClient webClient(){
    return WebClient.builder()
        .clientConnector(new ReactorClientHttpConnector(HttpClient.create()))
        .baseUrl(backendEndpoint)
        .build();
  }



}
