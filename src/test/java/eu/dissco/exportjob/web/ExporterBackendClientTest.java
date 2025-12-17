package eu.dissco.exportjob.web;

import static eu.dissco.exportjob.utils.TestUtils.DOWNLOAD_LINK;
import static eu.dissco.exportjob.utils.TestUtils.JOB_ID;
import static eu.dissco.exportjob.utils.TestUtils.MAPPER;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import eu.dissco.exportjob.domain.JobStateEndpoint;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import java.io.IOException;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;

@ExtendWith(MockitoExtension.class)
class ExporterBackendClientTest {

  private static MockWebServer mockServer;
  private ExporterBackendClient exporterBackendClient;

  @BeforeEach
  void setup() throws IOException {
    mockServer = new MockWebServer();
    mockServer.start();
    WebClient webClient = WebClient.create(
        String.format("http://%s:%s", mockServer.getHostName(), mockServer.getPort()));
    exporterBackendClient = new ExporterBackendClient(webClient, MAPPER);
  }

  @AfterEach
  void destroy() throws IOException {
    mockServer.shutdown();
  }

  @Test
  void testUpdateJobState() {
    mockServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.NO_CONTENT.value()));

    // When / Then
    assertDoesNotThrow(() -> exporterBackendClient.updateJobState(JOB_ID,
        JobStateEndpoint.RUNNING));
  }

  @Test
  void testUpdateJobStateFailedToNotify() {
    mockServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.BAD_GATEWAY.value()));

    // When / Then
    assertDoesNotThrow(() -> exporterBackendClient.updateJobState(JOB_ID,
        JobStateEndpoint.RUNNING));
  }

  @Test
  void testMarkJobAsComplete() {
    // Given
    mockServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.NO_CONTENT.value()));

    // When / Then
    assertDoesNotThrow(() -> exporterBackendClient.markJobAsComplete(JOB_ID, DOWNLOAD_LINK));
  }

  @Test
  void testMarkJobAsCompleteFailedToNotify() {
    // Given
    mockServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.BAD_GATEWAY.value()));

    // When / Then
    assertThrows(FailedProcessingException.class,
        () -> exporterBackendClient.markJobAsComplete(JOB_ID, DOWNLOAD_LINK));
  }

}
