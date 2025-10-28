package eu.dissco.exportjob.repository;

import static eu.dissco.exportjob.utils.TestUtils.DOI_2;
import static eu.dissco.exportjob.utils.TestUtils.MAPPER;
import static eu.dissco.exportjob.utils.TestUtils.ORG_1;
import static eu.dissco.exportjob.utils.TestUtils.ORG_2;
import static eu.dissco.exportjob.utils.TestUtils.PHYS_ID_2;
import static eu.dissco.exportjob.utils.TestUtils.givenDigitalSpecimen;
import static eu.dissco.exportjob.utils.TestUtils.givenDigitalSpecimenReducedDoiList;
import static eu.dissco.exportjob.utils.TestUtils.givenMediaJson;
import static eu.dissco.exportjob.utils.TestUtils.givenSearchParams;
import static eu.dissco.exportjob.utils.TestUtils.givenTargetFields;
import static org.assertj.core.api.Assertions.assertThat;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.exportjob.domain.SearchParam;
import eu.dissco.exportjob.domain.TargetType;
import eu.dissco.exportjob.properties.ElasticSearchProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
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
      "docker.elastic.co/elasticsearch/elasticsearch").withTag("9.2.0");
  private static final String ELASTICSEARCH_USERNAME = "elastic";
  private static final String ELASTICSEARCH_PASSWORD = "s3cret";
  private static final ElasticsearchContainer container = new ElasticsearchContainer(
      ELASTIC_IMAGE).withPassword(ELASTICSEARCH_PASSWORD);
  private static final String DIGITAL_SPECIMEN_INDEX = "digital-specimen";
  private static final String DIGITAL_MEDIA_INDEX = "digital-media";
  private static ElasticsearchClient client;
  private static Rest5Client restClient;
  private final ElasticSearchProperties properties = new ElasticSearchProperties();
  private ElasticSearchRepository elasticRepository;

  @BeforeAll
  static void initContainer() {
    // Create the elasticsearch container.
    container.start();

    var creds = Base64.getEncoder()
        .encodeToString((ELASTICSEARCH_USERNAME + ":" + ELASTICSEARCH_PASSWORD).getBytes());

    restClient = Rest5Client.builder(
            new HttpHost("https", "localhost", container.getMappedPort(9200)))
        .setDefaultHeaders(new Header[]{new BasicHeader("Authorization", "Basic " + creds)})
        .setSSLContext(container.createSslContextFromCa()).build();

    ElasticsearchTransport transport = new Rest5ClientTransport(restClient,
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
        DIGITAL_SPECIMEN_INDEX,
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
        DIGITAL_SPECIMEN_INDEX,
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
        DIGITAL_SPECIMEN_INDEX,
        List.of(givenDigitalSpecimen(), givenDigitalSpecimen(DOI_2, ORG_2, PHYS_ID_2), expected));
    var searchParam = List.of(new SearchParam("$['ods:organisationID']", null));

    // When
    var result = elasticRepository.getTargetObjects(searchParam, TargetType.DIGITAL_SPECIMEN, null,
        null);

    // Then
    assertThat(result).isEqualTo(List.of(expected));
  }

  @Test
  void testGetTargetForMediaList() throws IOException {
    // Given
    var mediaList = new ArrayList<JsonNode>();
    for (int i = 0; i < 10; i++) {
      var jsonNode = givenMediaJson();
      ((ObjectNode) jsonNode).put("@id", "https://doi.org/TEST/WVW-SCM-C9" + i);
      mediaList.add(jsonNode);
    }
    postDigitalSpecimens(DIGITAL_MEDIA_INDEX, mediaList);

    // When
    var result = elasticRepository.getTargetMediaById(
        mediaList.stream().map(node -> node.get("@id").asText()).toList());

    // Then
    assertThat(result).isEqualTo(mediaList);
  }

  private void postDigitalSpecimens(String indexName, List<JsonNode> jsonObjects)
      throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var jsonObject : jsonObjects) {
      bulkRequest.operations(op -> op.index(
          idx -> idx.index(indexName).id(jsonObject.get("@id").asText())
              .document(jsonObject)));
    }
    client.bulk(bulkRequest.build());
    client.indices().refresh(b -> b.index(indexName));
  }
}
