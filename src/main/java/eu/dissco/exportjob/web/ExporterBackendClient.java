package eu.dissco.exportjob.web;

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

  @Qualifier("exporter")
  private final WebClient webClient;

  public void updateJobState(UUID jobId, String path) {
    try {
      webClient
          .method(HttpMethod.POST)
          .uri(uriBuilder -> uriBuilder.path("/" + jobId.toString() + path).build())
          .retrieve()
          .toBodilessEntity().toFuture().get();
    } catch (ExecutionException | InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Unable to notify exporter backend that job {} is {}", jobId,
          path.replace("/", ""));
    }
  }

  // todo -> define body in backend
  public void markJobAsComplete(UUID jobId) {
    try {
      webClient
          .method(HttpMethod.POST)
          .uri(uriBuilder -> uriBuilder.path("/complete").build())
          .retrieve()
          .toBodilessEntity().toFuture().get();
    } catch (ExecutionException | InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Unable to notify exporter backend that job {} is running", jobId);
    }
  }

}
