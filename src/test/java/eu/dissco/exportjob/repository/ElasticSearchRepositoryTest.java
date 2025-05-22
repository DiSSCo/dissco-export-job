package eu.dissco.exportjob.repository;

import static eu.dissco.exportjob.utils.TestUtils.DOI_2;
import static eu.dissco.exportjob.utils.TestUtils.MAPPER;
import static eu.dissco.exportjob.utils.TestUtils.ORG_1;
import static eu.dissco.exportjob.utils.TestUtils.ORG_2;
import static eu.dissco.exportjob.utils.TestUtils.PHYS_ID_2;
import static eu.dissco.exportjob.utils.TestUtils.givenDigitalSpecimen;
import static eu.dissco.exportjob.utils.TestUtils.givenDigitalSpecimenReducedDoiList;
import static eu.dissco.exportjob.utils.TestUtils.givenSearchParams;
import static eu.dissco.exportjob.utils.TestUtils.givenTargetFields;
import static org.assertj.core.api.Assertions.assertThat;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.exportjob.domain.SearchParam;
import eu.dissco.exportjob.domain.TargetType;
import eu.dissco.exportjob.properties.ElasticSearchProperties;
import java.io.IOException;
import java.util.List;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ElasticSearchRepositoryTest {

  private static final DockerImageName ELASTIC_IMAGE = DockerImageName.parse(
      "docker.elastic.co/elasticsearch/elasticsearch").withTag("8.7.1");
  private static final String ELASTICSEARCH_USERNAME = "elastic";
  private static final String ELASTICSEARCH_PASSWORD = "s3cret";
  private static final ElasticsearchContainer container = new ElasticsearchContainer(
      ELASTIC_IMAGE).withPassword(ELASTICSEARCH_PASSWORD);
  private static ElasticsearchClient client;
  private static RestClient restClient;
  private final ElasticSearchProperties properties = new ElasticSearchProperties();
  private ElasticSearchRepository elasticRepository;
  private static final String DIGITAL_SPECIMEN_INDEX = "digital-specimen";
  private static final String DIGITAL_MEDIA_INDEX = "digital-media";

  @BeforeAll
  static void initContainer() {
    // Create the elasticsearch container.
    container.start();

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD));

    HttpHost host = new HttpHost("localhost",
        container.getMappedPort(9200), "https");
    final RestClientBuilder builder = RestClient.builder(host);

    builder.setHttpClientConfigCallback(clientBuilder -> {
      clientBuilder.setSSLContext(container.createSslContextFromCa());
      clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      return clientBuilder;
    });
    restClient = builder.build();

    ElasticsearchTransport transport = new RestClientTransport(restClient,
        new JacksonJsonpMapper(MAPPER));

    client = new ElasticsearchClient(transport);
  }

  @AfterAll
  static void closeResources() throws Exception {
    restClient.close();
  }

  @BeforeEach
  void initRepository() {
    elasticRepository = new ElasticSearchRepository(client, properties);
  }

  @AfterEach
  void clearIndex() throws IOException {
    if (client.indices().exists(re -> re.index(DIGITAL_SPECIMEN_INDEX)).value()) {
      client.indices().delete(b -> b.index(DIGITAL_SPECIMEN_INDEX));
    }
    if (client.indices().exists(re -> re.index(DIGITAL_MEDIA_INDEX)).value()) {
      client.indices().delete(b -> b.index(DIGITAL_MEDIA_INDEX));
    }
  }

  @Test
  void testGetTargetObject() throws IOException {
    // Given
    postDigitalSpecimens(
        List.of(givenDigitalSpecimen(), givenDigitalSpecimen(DOI_2, ORG_2, PHYS_ID_2)));

    // When
    var result = elasticRepository.getTargetObjects(givenSearchParams(),
        TargetType.DIGITAL_SPECIMEN, null, givenTargetFields());

    // Then
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenReducedDoiList()));
  }

  @Test
  void testGetTargetObjectSecondPage() throws IOException {
    // Given
    postDigitalSpecimens(
        List.of(givenDigitalSpecimen(), givenDigitalSpecimen(DOI_2, ORG_1, PHYS_ID_2)));

    // When
    var result = elasticRepository.getTargetObjects(givenSearchParams(),
        TargetType.DIGITAL_SPECIMEN, DOI_2, givenTargetFields());

    // Then
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenReducedDoiList()));
  }

  @Test
  void testGetTargetObjectEmptyOrg() throws IOException {
    // Given
    var expected = ((ObjectNode) (givenDigitalSpecimen("doi.org/1", ORG_2, PHYS_ID_2)));
    expected.remove("ods:organisationID");
    postDigitalSpecimens(
        List.of(givenDigitalSpecimen(), givenDigitalSpecimen(DOI_2, ORG_2, PHYS_ID_2), expected));
    var searchParam = List.of(new SearchParam("$['ods:organisationID']", null));

    // When
    var result = elasticRepository.getTargetObjects(searchParam, TargetType.DIGITAL_SPECIMEN, null, null);

    // Then
    assertThat(result).isEqualTo(List.of(expected));
  }

  private void postDigitalSpecimens(List<JsonNode> digitalSpecimens) throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var digitalSpecimen : digitalSpecimens) {
      bulkRequest.operations(op -> op.index(
          idx -> idx.index(DIGITAL_SPECIMEN_INDEX).id(digitalSpecimen.get("@id").asText())
              .document(digitalSpecimen)));
    }
    client.bulk(bulkRequest.build());
    client.indices().refresh(b -> b.index(DIGITAL_SPECIMEN_INDEX));
  }
}
