package eu.dissco.exportjob.service;

import static eu.dissco.exportjob.utils.TestUtils.DOWNLOAD_LINK;
import static eu.dissco.exportjob.utils.TestUtils.JOB_ID;
import static eu.dissco.exportjob.utils.TestUtils.MAPPER;
import static eu.dissco.exportjob.utils.TestUtils.givenJobRequest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.exportjob.domain.JobStateEndpoint;
import eu.dissco.exportjob.properties.DwcDpProperties;
import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.properties.JobProperties;
import eu.dissco.exportjob.repository.DatabaseRepository;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.web.ExporterBackendClient;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DwcDpServiceTest {

  private DwcDpService service;
  @Mock
  private ElasticSearchRepository elasticSearchRepository;
  @Mock
  private ExporterBackendClient exporterBackendClient;
  @Mock
  private S3Repository s3Repository;
  @Mock
  private IndexProperties indexProperties;
  @Mock
  private DatabaseRepository databaseRepository;
  @Mock
  private JobProperties jobProperties;

  @BeforeEach
  void setup() {
    service = new DwcDpService(elasticSearchRepository, exporterBackendClient, s3Repository,
        indexProperties, MAPPER, databaseRepository, jobProperties, new DwcDpProperties());
  }

  @AfterEach
  void tearDown() throws IOException {
    var path = Path.of("src/test/resources/temp.zip");
    if (path.toFile().exists()) {
      Files.delete(path);
    }
  }

  @Test
  void testHandleMessage() throws Exception {
    // Given
    given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
        List.of(givenSpecimenData()));
    given(elasticSearchRepository.getTargetForMediaList(List.of("https://doi.org/TEST/Y9H-N1L-J7G",
        "https://doi.org/TEST/WVW-SCM-C9Z"))).willReturn(List.of(givenMedia()));
    given(jobProperties.getJobId()).willReturn(JOB_ID);
    given(indexProperties.getTempFileLocation()).willReturn("src/test/resources/temp.zip");
    given(s3Repository.uploadResults(any(), eq(JOB_ID))).willReturn(DOWNLOAD_LINK);
    var materialByte = HexFormat.of().parseHex(
        "ACED00057372002865752E64697373636F2E6578706F72746A6F622E646F6D61696E2E44774344504D6174657269616C000000000000000102002F4C00136173736F63696174656453657175656E6365737400124C6A6176612F6C616E672F537472696E673B4C000D636174616C6F674E756D62657271007E00014C000B636F6C6C6563746564427971007E00014C000D636F6C6C65637465644279494471007E00014C000E636F6C6C656374696F6E436F646571007E00014C000C636F6C6C656374696F6E494471007E00014C00136461746147656E6572616C697A6174696F6E7371007E00014C000E646174654964656E74696669656471007E00014C001164657269766174696F6E4576656E74494471007E00014C000E64657269766174696F6E5479706571007E00014C001B6465726976656446726F6D4D6174657269616C456E74697479494471007E00014C000B646973706F736974696F6E71007E00014C00076576656E74494471007E00014C001765766964656E6365466F724F6363757272656E6365494471007E00014C0018686967686572436C617373696669636174696F6E4E616D6571007E00014C0018686967686572436C617373696669636174696F6E52616E6B71007E00014C00186964656E74696669636174696F6E5265666572656E63657371007E00014C00156964656E74696669636174696F6E52656D61726B7371007E00014C00206964656E74696669636174696F6E566572696669636174696F6E53746174757371007E00014C000C6964656E746966696564427971007E00014C000E6964656E7469666965644279494471007E00014C0013696E666F726D6174696F6E5769746868656C6471007E00014C000F696E737469747574696F6E436F646571007E00014C000D696E737469747574696F6E494471007E00014C00186973506172744F664D6174657269616C456E74697479494471007E00014C00106D6174657269616C43617465676F727971007E00014C00106D6174657269616C456E74697479494471007E00014C00156D6174657269616C456E7469747952656D61726B7371007E00014C00126D6174657269616C456E746974795479706571007E00014C00126D6174657269616C5265666572656E63657371007E00014C000E6F626A6563745175616E7469747971007E00014C00126F626A6563745175616E746974795479706571007E00014C00136F74686572436174616C6F674E756D6265727371007E00014C00146F776E6572496E737469747574696F6E436F646571007E00014C00126F776E6572496E737469747574696F6E494471007E00014C000C7072657061726174696F6E7371007E00014C000C7265636F72644E756D62657271007E00014C000E736369656E74696669634E616D6571007E00014C000C7461786F6E466F726D756C6171007E00014C00077461786F6E494471007E00014C00097461786F6E52616E6B71007E00014C000C7461786F6E52656D61726B7371007E00014C00137479706544657369676E6174696F6E5479706571007E00014C000A7479706553746174757371007E00014C000C74797069666965644E616D6571007E00014C0016766572626174696D4964656E74696669636174696F6E71007E00014C000D766572626174696D4C6162656C71007E00017870707070707074004E68747470733A2F2F7777772E676269662E6F72672F6772736369636F6C6C2F636F6C6C656374696F6E2F61666638623834342D623938662D343234652D623962392D39623037383432336439663370707070707074000B2D313839373730343138377070707070707070707074001968747470733A2F2F726F722E6F72672F303430636B32623836707074002068747470733A2F2F646F692E6F72672F544553542F57344B2D5143362D354835707070707070707074000A546973737565202D20317070707070707070707070");
    given(databaseRepository.getRecords(anyString(), eq(0), eq(10000))).willReturn(
        List.of());
    given(databaseRepository.getRecords("temp_table_cd5c9ee7_material", 0, 10000)).willReturn(
        List.of(materialByte));

    // When
    service.handleMessage(givenJobRequest());

    // Then
    then(elasticSearchRepository).should().shutdown();
    then(exporterBackendClient).should().markJobAsComplete(JOB_ID, DOWNLOAD_LINK);
  }

  @Test
  void testHandleMessageException() throws Exception {
    // Given
    given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
        List.of(givenSpecimenData()));
    given(elasticSearchRepository.getTargetForMediaList(List.of("https://doi.org/TEST/Y9H-N1L-J7G",
        "https://doi.org/TEST/WVW-SCM-C9Z"))).willReturn(List.of(givenMedia()));
    given(jobProperties.getJobId()).willReturn(JOB_ID);
    given(indexProperties.getTempFileLocation()).willReturn("///src/test/resources/temp.zip");

    // When
    service.handleMessage(givenJobRequest());

    // Then
    then(elasticSearchRepository).should().shutdown();
    then(exporterBackendClient).should().updateJobState(JOB_ID, JobStateEndpoint.FAILED);
  }

  @Test
  void testCreateTables() {
    // Given
    given(jobProperties.getJobId()).willReturn(JOB_ID);

    // When
    service.setup();

    // Then
    then(databaseRepository).should(times(13)).createTable(anyString());
  }

  @Test
  void testDestroyTables() {
    // Given
    given(jobProperties.getJobId()).willReturn(JOB_ID);

    // When
    service.cleanup();

    // Then
    then(databaseRepository).should(times(13)).dropTable(anyString());
  }

  private JsonNode givenSpecimenData() throws JsonProcessingException {
    return MAPPER.readTree(
        """
            {
              "@id": "https://doi.org/TEST/W4K-QC6-5H5",
              "@type": "ods:DigitalSpecimen",
              "dcterms:identifier": "https://doi.org/TEST/W4K-QC6-5H5",
              "ods:version": 3,
              "ods:status": "Active",
              "dcterms:modified": "2020-11-19",
              "dcterms:created": "2025-05-08T13:20:19.837Z",
              "ods:fdoType": "https://doi.org/21.T11148/894b1e6cad57e921764e",
              "ods:midsLevel": 2,
              "ods:normalisedPhysicalSpecimenID": "79569268-d66d-4899-b3f7-aafeb13069d0",
              "ods:physicalSpecimenID": "79569268-d66d-4899-b3f7-aafeb13069d0",
              "ods:physicalSpecimenIDType": "Global",
              "ods:isKnownToContainMedia": true,
              "ods:sourceSystemID": "https://hdl.handle.net/TEST/Z1M-8WG-DCD",
              "ods:sourceSystemName": "NHMD Ornithology Collection",
              "ods:livingOrPreserved": "Preserved",
              "dcterms:license": "http://creativecommons.org/licenses/by/4.0/legalcode",
              "dwc:basisOfRecord": "PreservedSpecimen",
              "ods:organisationID": "https://ror.org/040ck2b86",
              "ods:organisationName": "Natural History Museum of Denmark",
              "dwc:collectionID": "https://www.gbif.org/grscicoll/collection/aff8b844-b98f-424e-b9b9-9b078423d9f3",
              "ods:topicOrigin": "Natural",
              "ods:topicDomain": "Life",
              "ods:topicDiscipline": "Zoology",
              "ods:specimenName": "Crateroscelis murina (P. L. Sclater, 1858)",
              "dwc:preparations": "Tissue - 1",
              "dwc:datasetName": "Natural History Museum Denmark | Ornithology Collection",
              "ods:hasEntityRelationships": [
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasDigitalMedia",
                  "dwc:relatedResourceID": "TEST/Y9H-N1L-J7G",
                  "ods:relatedResourceURI": "https://doi.org/TEST/Y9H-N1L-J7G",
                  "dwc:relationshipEstablishedDate": "2025-05-08T13:20:19.837Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14383054",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14383054",
                      "schema:name": "DiSSCo Digital Specimen Processing Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "processing-service"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14383054",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14383054",
                          "ods:isPartOfLabel": false,
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                },
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasDigitalMedia",
                  "dwc:relatedResourceID": "TEST/WVW-SCM-C9Z",
                  "ods:relatedResourceURI": "https://doi.org/TEST/WVW-SCM-C9Z",
                  "dwc:relationshipEstablishedDate": "2025-05-08T13:20:19.837Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14383054",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14383054",
                      "schema:name": "DiSSCo Digital Specimen Processing Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "processing-service"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14383054",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14383054",
                          "ods:isPartOfLabel": false,
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                },
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasOrganisationID",
                  "dwc:relatedResourceID": "https://ror.org/040ck2b86",
                  "ods:relatedResourceURI": "https://ror.org/040ck2b86",
                  "dwc:relationshipEstablishedDate": "2025-05-07T14:16:46.170Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14379776",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14379776",
                      "schema:name": "DiSSCo Translator Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "data-translator"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14379776",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14379776",
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                },
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasSourceSystemID",
                  "dwc:relatedResourceID": "https://hdl.handle.net/TEST/Z1M-8WG-DCD",
                  "ods:relatedResourceURI": "https://hdl.handle.net/TEST/Z1M-8WG-DCD",
                  "dwc:relationshipEstablishedDate": "2025-05-07T14:16:46.170Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14379776",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14379776",
                      "schema:name": "DiSSCo Translator Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "data-translator"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14379776",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14379776",
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                },
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasFDOType",
                  "dwc:relatedResourceID": "https://doi.org/21.T11148/894b1e6cad57e921764e",
                  "ods:relatedResourceURI": "https://doi.org/21.T11148/894b1e6cad57e921764e",
                  "dwc:relationshipEstablishedDate": "2025-05-07T14:16:46.170Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14379776",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14379776",
                      "schema:name": "DiSSCo Translator Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "data-translator"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14379776",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14379776",
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                },
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasLicense",
                  "dwc:relatedResourceID": "http://creativecommons.org/licenses/by/4.0/legalcode",
                  "ods:relatedResourceURI": "http://creativecommons.org/licenses/by/4.0/legalcode",
                  "dwc:relationshipEstablishedDate": "2025-05-07T14:16:46.170Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14379776",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14379776",
                      "schema:name": "DiSSCo Translator Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "data-translator"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14379776",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14379776",
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                },
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasCollectionID",
                  "dwc:relatedResourceID": "https://www.gbif.org/grscicoll/collection/aff8b844-b98f-424e-b9b9-9b078423d9f3",
                  "ods:relatedResourceURI": "https://www.gbif.org/grscicoll/collection/aff8b844-b98f-424e-b9b9-9b078423d9f3",
                  "dwc:relationshipEstablishedDate": "2025-05-07T14:16:46.170Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14379776",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14379776",
                      "schema:name": "DiSSCo Translator Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "data-translator"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14379776",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14379776",
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                },
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasCOLID",
                  "dwc:relatedResourceID": "Z938",
                  "ods:relatedResourceURI": "https://www.catalogueoflife.org/data/taxon/Z938",
                  "dwc:relationshipEstablishedDate": "2025-05-07T14:18:18.959Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14380476",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14380476",
                      "schema:name": "DiSSCo Name Usage Search Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "taxon-resolver"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14380476",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14380476",
                          "ods:isPartOfLabel": false,
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                },
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasCOLID",
                  "dwc:relatedResourceID": "Z938",
                  "ods:relatedResourceURI": "https://www.catalogueoflife.org/data/taxon/Z938",
                  "dwc:relationshipEstablishedDate": "2025-05-07T14:18:18.959Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14380476",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14380476",
                      "schema:name": "DiSSCo Name Usage Search Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "taxon-resolver"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14380476",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14380476",
                          "ods:isPartOfLabel": false,
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                }
              ],
              "ods:hasIdentifications": [
                {
                  "@id": "3d049684-9e93-404c-8071-f22e4c836154",
                  "@type": "ods:Identification",
                  "dwc:identificationID": "3d049684-9e93-404c-8071-f22e4c836154",
                  "ods:identificationType": "TaxonIdentification",
                  "dwc:verbatimIdentification": "Crateroscelis murina",
                  "ods:isVerifiedIdentification": true,
                  "ods:hasTaxonIdentifications": [
                    {
                      "@id": "https://www.catalogueoflife.org/data/taxon/Z938",
                      "@type": "ods:TaxonIdentification",
                      "dwc:taxonID": "https://www.catalogueoflife.org/data/taxon/Z938",
                      "dwc:scientificName": "Crateroscelis murina (P. L. Sclater, 1858)",
                      "ods:scientificNameHTMLLabel": "<i>Crateroscelis murina</i> (P. L. Sclater, 1858)",
                      "ods:genusHTMLLabel": "<i>Crateroscelis</i> Sharpe, 1883",
                      "dwc:scientificNameAuthorship": "(P. L. Sclater, 1858)",
                      "dwc:taxonRank": "SPECIES",
                      "dwc:kingdom": "Animalia",
                      "dwc:phylum": "Chordata",
                      "dwc:class": "Aves",
                      "dwc:order": "Passeriformes",
                      "dwc:family": "Acanthizidae Bonaparte, 1854",
                      "dwc:genus": "Crateroscelis Sharpe, 1883",
                      "dwc:specificEpithet": "murina",
                      "dwc:taxonomicStatus": "ACCEPTED",
                      "dwc:genericName": "Crateroscelis"
                    }
                  ]
                },
                {
                  "@type": "ods:Identification",
                  "ods:identificationType": "TaxonIdentification",
                  "dwc:verbatimIdentification": "Crateroscelis murina",
                  "ods:hasTaxonIdentifications": [
                    {
                      "@id": "https://www.catalogueoflife.org/data/taxon/Z938",
                      "@type": "ods:TaxonIdentification",
                      "dwc:taxonID": "https://www.catalogueoflife.org/data/taxon/Z938",
                      "dwc:scientificName": "Crateroscelis murina (P. L. Sclater, 1858)",
                      "ods:scientificNameHTMLLabel": "<i>Crateroscelis murina</i> (P. L. Sclater, 1858)",
                      "ods:genusHTMLLabel": "<i>Crateroscelis</i> Sharpe, 1883",
                      "dwc:scientificNameAuthorship": "(P. L. Sclater, 1858)",
                      "dwc:taxonRank": "SPECIES",
                      "dwc:kingdom": "Animalia",
                      "dwc:phylum": "Chordata",
                      "dwc:class": "Aves",
                      "dwc:order": "Passeriformes",
                      "dwc:family": "Acanthizidae Bonaparte, 1854",
                      "dwc:genus": "Crateroscelis Sharpe, 1883",
                      "dwc:specificEpithet": "murina",
                      "dwc:taxonomicStatus": "ACCEPTED",
                      "dwc:acceptedNameUsage": "Crateroscelis murina",
                      "dwc:genericName": "Crateroscelis"
                    }
                  ],
                  "ods:hasAgents": [
                    {
                      "@type": "schema:Person",
                      "schema:name": "Jønsson, Knud",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "identifier"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://orcid.org/0000-0002-5669-2769",
                          "@type": "ods:Identifier",
                          "dcterms:title": "orcid",
                          "dcterms:identifier": "https://orcid.org/0000-0002-5669-2769",
                          "ods:gupriLevel": "GloballyUniqueStable"
                        }
                      ]
                    }
                  ]
                }
              ],
              "ods:hasIdentifiers": [
                {
                  "@id": "79569268-d66d-4899-b3f7-aafeb13069d0",
                  "@type": "ods:Identifier",
                  "dcterms:title": "dwc:occurrenceID",
                  "dcterms:type": "UUID",
                  "dcterms:identifier": "79569268-d66d-4899-b3f7-aafeb13069d0",
                  "ods:gupriLevel": "GloballyUniqueStable"
                },
                {
                  "@id": "79569268-d66d-4899-b3f7-aafeb13069d0",
                  "@type": "ods:Identifier",
                  "dcterms:title": "dwca:ID",
                  "dcterms:type": "UUID",
                  "dcterms:identifier": "79569268-d66d-4899-b3f7-aafeb13069d0",
                  "ods:gupriLevel": "GloballyUniqueStable"
                },
                {
                  "@id": "NHMD616060",
                  "@type": "ods:Identifier",
                  "dcterms:title": "dwc:catalogNumber",
                  "dcterms:type": "Locally unique identifier",
                  "dcterms:identifier": "NHMD616060",
                  "ods:gupriLevel": "LocallyUniqueStable"
                }
              ],
              "ods:hasEvents": [
                {
                  "@type": "ods:Event",
                  "dwc:eventType": "Collecting Event",
                  "dwc:fieldNumber": "KAJ2-16.11.18",
                  "dwc:eventDate": "2018-11-16",
                  "ods:hasAgents": [
                    {
                      "@type": "schema:Person",
                      "schema:name": "Jønsson, Knud",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "collector"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://orcid.org/0000-0002-5669-2769",
                          "@type": "ods:Identifier",
                          "dcterms:title": "orcid",
                          "dcterms:identifier": "https://orcid.org/0000-0002-5669-2769",
                          "ods:gupriLevel": "GloballyUniqueStable"
                        }
                      ]
                    },
                    {
                      "@type": "schema:Person",
                      "schema:name": "Reeve, Andrew",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "collector"
                        }
                      ]
                    },
                    {
                      "@type": "schema:Person",
                      "schema:name": "Bodawatta, Kasun",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "collector"
                        }
                      ]
                    }
                  ],
                  "ods:hasLocation": {
                    "@type": "ods:Location",
                    "dwc:continent": "Oceania",
                    "dwc:country": "Papua New Guinea",
                    "dwc:locality": "Yawan Village, Huon Peninsula",
                    "dwc:higherGeography": "Papua New Guinea",
                    "ods:hasGeoreference": {
                      "@type": "ods:Georeference",
                      "dwc:decimalLatitude": -6.1325,
                      "dwc:decimalLongitude": 146.84255,
                      "dwc:geodeticDatum": "WGS84"
                    }
                  }
                }
              ]
            }
            """
    );
  }

  private JsonNode givenMedia() throws JsonProcessingException {
    return MAPPER.readTree(
        """
                        {
              "@id": "https://doi.org/TEST/WVW-SCM-C9Z",
              "@type": "ods:DigitalMedia",
              "dcterms:identifier": "https://doi.org/TEST/WVW-SCM-C9Z",
              "ods:fdoType": "https://doi.org/21.T11148/bbad8c4e101e8af01115",
              "ods:version": 1,
              "ods:status": "Active",
              "dcterms:modified": "2025-05-08T13:18:33.798Z",
              "dcterms:created": "2025-05-08T13:20:20.167Z",
              "dcterms:type": "StillImage",
              "ac:accessURI": "https://specify-media.science.ku.dk/fileget?coll=NHMD+Ornithology&type=O&filename=sp68168655423943006713.att.JPG",
              "ods:sourceSystemID": "https://hdl.handle.net/TEST/Z1M-8WG-DCD",
              "ods:sourceSystemName": "NHMD Ornithology Collection",
              "ods:organisationID": "https://ror.org/040ck2b86",
              "ods:organisationName": "Natural History Museum of Denmark",
              "dcterms:format": "image/jpeg",
              "dcterms:title": "DSC_0520",
              "dcterms:rights": "CC BY 4.0",
              "ods:hasIdentifiers": [
                {
                  "@id": "79569268-d66d-4899-b3f7-aafeb13069d0",
                  "@type": "ods:Identifier",
                  "dcterms:title": "dwca:ID",
                  "dcterms:type": "UUID",
                  "dcterms:identifier": "79569268-d66d-4899-b3f7-aafeb13069d0",
                  "ods:gupriLevel": "GloballyUniqueStable"
                },
                {
                  "@id": "https://specify-media.science.ku.dk/fileget?coll=NHMD+Ornithology&type=O&filename=sp68168655423943006713.att.JPG",
                  "@type": "ods:Identifier",
                  "dcterms:title": "dcterms:identifier",
                  "dcterms:type": "URL",
                  "dcterms:identifier": "https://specify-media.science.ku.dk/fileget?coll=NHMD+Ornithology&type=O&filename=sp68168655423943006713.att.JPG",
                  "ods:gupriLevel": "GloballyUniqueStablePersistentResolvable"
                }
              ],
              "ods:hasEntityRelationships": [
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasURL",
                  "dwc:relatedResourceID": "https://specify-media.science.ku.dk/fileget?coll=NHMD+Ornithology&type=O&filename=sp68168655423943006713.att.JPG",
                  "ods:relatedResourceURI": "https://specify-media.science.ku.dk/fileget?coll=NHMD+Ornithology&type=O&filename=sp68168655423943006713.att.JPG",
                  "dwc:relationshipEstablishedDate": "2025-05-08T13:18:33.798Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14379776",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14379776",
                      "schema:name": "DiSSCo Translator Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "data-translator"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14379776",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14379776",
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                },
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasOrganisationID",
                  "dwc:relatedResourceID": "https://ror.org/040ck2b86",
                  "ods:relatedResourceURI": "https://ror.org/040ck2b86",
                  "dwc:relationshipEstablishedDate": "2025-05-08T13:18:33.798Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14379776",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14379776",
                      "schema:name": "DiSSCo Translator Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "data-translator"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14379776",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14379776",
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                },
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasFDOType",
                  "dwc:relatedResourceID": "https://doi.org/21.T11148/bbad8c4e101e8af01115",
                  "ods:relatedResourceURI": "https://doi.org/21.T11148/bbad8c4e101e8af01115",
                  "dwc:relationshipEstablishedDate": "2025-05-08T13:18:33.798Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14379776",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14379776",
                      "schema:name": "DiSSCo Translator Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "data-translator"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14379776",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14379776",
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                },
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasSourceSystemID",
                  "dwc:relatedResourceID": "https://hdl.handle.net/TEST/Z1M-8WG-DCD",
                  "ods:relatedResourceURI": "https://hdl.handle.net/TEST/Z1M-8WG-DCD",
                  "dwc:relationshipEstablishedDate": "2025-05-08T13:18:33.798Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14379776",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14379776",
                      "schema:name": "DiSSCo Translator Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "data-translator"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14379776",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14379776",
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                },
                {
                  "@type": "ods:EntityRelationship",
                  "dwc:relationshipOfResource": "hasDigitalSpecimen",
                  "dwc:relatedResourceID": "TEST/W4K-QC6-5H5",
                  "ods:relatedResourceURI": "https://doi.org/TEST/W4K-QC6-5H5",
                  "dwc:relationshipEstablishedDate": "2025-05-08T13:20:20.167Z",
                  "ods:hasAgents": [
                    {
                      "@id": "https://doi.org/10.5281/zenodo.14383054",
                      "@type": "schema:SoftwareApplication",
                      "schema:identifier": "https://doi.org/10.5281/zenodo.14383054",
                      "schema:name": "DiSSCo Digital Specimen Processing Service",
                      "ods:hasRoles": [
                        {
                          "@type": "schema:Role",
                          "schema:roleName": "processing-service"
                        }
                      ],
                      "ods:hasIdentifiers": [
                        {
                          "@id": "https://doi.org/10.5281/zenodo.14383054",
                          "@type": "ods:Identifier",
                          "dcterms:title": "DOI",
                          "dcterms:type": "DOI",
                          "dcterms:identifier": "https://doi.org/10.5281/zenodo.14383054",
                          "ods:isPartOfLabel": false,
                          "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                          "ods:identifierStatus": "Preferred"
                        }
                      ]
                    }
                  ]
                }
              ]
            }
            """
    );
  }
}
