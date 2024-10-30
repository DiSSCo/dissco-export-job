package eu.dissco.exportjob.web;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.properties.TokenProperties;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
@RequiredArgsConstructor
@Slf4j
public class TokenAuthenticator {

  @Qualifier("tokenClient")
  private final WebClient tokenClient;
  private final TokenProperties properties;

  public String getToken() throws FailedProcessingException {
    var response = tokenClient
        .post()
        .body(BodyInserters.fromFormData(properties.getFromFormData()))
        .acceptCharset(StandardCharsets.UTF_8)
        .retrieve()
        .onStatus(HttpStatus.UNAUTHORIZED::equals,
            r -> Mono.error(new FailedProcessingException("Service is unauthorized.")))
        .bodyToMono(JsonNode.class)
        .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(2))
            .filter(TokenAuthenticator::is5xxServerError)
            .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) ->
                new FailedProcessingException(
                    "Token Authentication failed to process after max retries")
            ));
    try {
      var tokenNode = response.toFuture().get();
      return getToken(tokenNode);
    } catch (InterruptedException | ExecutionException e) {
      Thread.currentThread().interrupt();
      log.error("Token authentication: Unable to authenticate processing service with Keycloak. Verify client secret is up to-date");
      throw new FailedProcessingException(
          "Unable to authenticate processing service with Keycloak. More information: "
              + e.getMessage());
    }
  }

  private String getToken(JsonNode tokenNode) throws FailedProcessingException {
    if (tokenNode != null && tokenNode.get("access_token") != null) {
      return tokenNode.get("access_token").asText();
    }
    log.debug("Unexpected response from keycloak server. Unable to parse access_token");
    throw new FailedProcessingException(
        "Unable to authenticate processing service with Keycloak. An error has occurred parsing keycloak response");
  }

  private static boolean is5xxServerError(Throwable throwable) {
    return throwable instanceof WebClientResponseException webClientResponseException
        && webClientResponseException.getStatusCode().is5xxServerError();
  }

}
