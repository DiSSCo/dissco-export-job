package eu.dissco.exportjob.web;

import eu.dissco.exportjob.exceptions.FailedProcessingException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Component
@RequiredArgsConstructor
@Slf4j
public class ExporterBackendClient {

  @Qualifier("exporterClient")
  private final WebClient webClient;
  private final TokenAuthenticator tokenAuthenticator;

  public void updateJobState(UUID jobId, String path) throws FailedProcessingException {
    try {
      webClient
          .method(HttpMethod.POST)
          .uri(uriBuilder -> uriBuilder.path("/" + jobId.toString() + path).build())
          .header("Authorization", "Bearer " + tokenAuthenticator.getToken())
          .retrieve()
          .toBodilessEntity().toFuture().get();
    } catch (ExecutionException e) {
      log.error("Unable to notify exporter backend that job {} is {}", jobId,
          path.replace("/", ""));
    } catch (InterruptedException e) {
      log.error("Thread has been interrupted", e);
      Thread.currentThread().interrupt();
      throw new FailedProcessingException();
    }
  }

  // todo -> define body in backend
  public void markJobAsComplete(UUID jobId) throws FailedProcessingException {
    try {
      webClient
          .method(HttpMethod.POST)
          .uri(uriBuilder -> uriBuilder.path("/complete").build())
          .header("Authorization", "Bearer " + tokenAuthenticator.getToken())
          .retrieve()
          .toBodilessEntity().toFuture().get();
    } catch (ExecutionException  e) {
      log.error("Unable to notify exporter backend that job {} is running", jobId);
    } catch (InterruptedException e){
      Thread.currentThread().interrupt();
      log.error("Thread has been interrupted", e);
      throw new FailedProcessingException();
    }
  }

}
