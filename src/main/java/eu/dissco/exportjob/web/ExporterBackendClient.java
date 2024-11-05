package eu.dissco.exportjob.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.exportjob.domain.JobStateEndpoint;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

@Component
@RequiredArgsConstructor
@Slf4j
public class ExporterBackendClient {

  @Qualifier("exporterClient")
  private final WebClient webClient;
  private final ObjectMapper mapper;
  private final TokenAuthenticator tokenAuthenticator;

  public void updateJobState(UUID jobId, JobStateEndpoint stateEndpoint) throws FailedProcessingException {
    var endpoint = stateEndpoint.getEndpoint();
    try {
      webClient
          .method(HttpMethod.POST)
          .uri(uriBuilder -> uriBuilder.path("/" + jobId.toString() + endpoint).build())
          .header("Authorization", "Bearer " + tokenAuthenticator.getToken())
          .retrieve()
          .toBodilessEntity().toFuture().get();
    } catch (ExecutionException e) {
      log.error("Unable to notify exporter backend that job {} is {}", jobId,
          endpoint.replace("/", ""));
    } catch (InterruptedException e) {
      log.error("Thread has been interrupted", e);
      Thread.currentThread().interrupt();
      throw new FailedProcessingException();
    }
  }

  public void markJobAsComplete(UUID jobId, String downloadLink) throws FailedProcessingException {
    var body = mapper.createObjectNode()
        .put("id", jobId.toString())
        .put("downloadLink", downloadLink);
    try {
      webClient
          .method(HttpMethod.POST)
          .uri(uriBuilder -> uriBuilder.path("/completed").build())
          .header("Authorization", "Bearer " + tokenAuthenticator.getToken())
          .body(BodyInserters.fromValue(body))
          .retrieve()
          .toBodilessEntity().toFuture().get();
    } catch (ExecutionException  e) {
      log.error("Unable to notify exporter backend that job {} is complete", jobId, e);
    } catch (InterruptedException e){
      Thread.currentThread().interrupt();
      log.error("Thread has been interrupted", e);
      throw new FailedProcessingException();
    }
  }

}
