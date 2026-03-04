package eu.dissco.exportjob.client;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.service.annotation.PostExchange;
import tools.jackson.databind.JsonNode;

public interface ExporterBackendClient {

    @PostExchange("/{jobId}/{stateEndpoint}")
    void updateJobState(@PathVariable String jobId, @PathVariable String stateEndpoint);

    @PostExchange("/completed")
    void markJobAsComplete(@RequestBody JsonNode body);
}
