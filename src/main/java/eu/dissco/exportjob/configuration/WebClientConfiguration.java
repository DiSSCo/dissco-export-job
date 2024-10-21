package eu.dissco.exportjob.configuration;

import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;


@Configuration
@RequiredArgsConstructor
public class WebClientConfiguration {

  @NotBlank
  @Value("${endpoint.backend}")
  private String backendEndpoint;

  @NotBlank
  @Value("${endpoint.token}")
  private String tokenEndpoint;

  @Bean(name= "exporterClient")
  public WebClient webClient(){
    return WebClient.builder()
        .clientConnector(new ReactorClientHttpConnector(HttpClient.create()))
        .baseUrl(backendEndpoint)
        .defaultHeader(HttpHeaders.CONTENT_TYPE)
        .build();
  }

  @Bean(name = "tokenClient")
  public WebClient tokenClient() {
    return WebClient.builder()
        .clientConnector(new ReactorClientHttpConnector(HttpClient.create()))
        .baseUrl(tokenEndpoint)
        .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
        .build();
  }


}
