package eu.dissco.exportjob.service;

import eu.dissco.exportjob.component.DataPackageComponent;
import eu.dissco.exportjob.component.JobRequestComponent;
import eu.dissco.exportjob.domain.JobStateEndpoint;
import eu.dissco.exportjob.properties.DwcDpProperties;
import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.properties.JobProperties;
import eu.dissco.exportjob.properties.S3Properties;
import eu.dissco.exportjob.repository.DatabaseRepository;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.repository.SourceSystemRepository;
import freemarker.template.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;
import tools.jackson.databind.JsonNode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

import static eu.dissco.exportjob.utils.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class DwcDpServiceTest {

    private final Configuration configuration = new Configuration(Configuration.VERSION_2_3_32);

    private static final String HEX = "ACED00057372002E65752E64697373636F2E6578706F72746A6F622E646F6D61696E2E64776364702E44774344704D6174657269616C00000000000000010200484C00136173736F63696174656453657175656E6365737400124C6A6176612F6C616E672F537472696E673B4C000D636174616C6F674E756D62657271007E00014C0014636C617373696669636174696F6E53797374656D71007E00014C000B636F6C6C6563746564427971007E00014C000D636F6C6C65637465644279494471007E00014C000E636F6C6C656374656442795F666B71007E00014C000E636F6C6C656374696F6E436F646571007E00014C0012636F6C6C656374696F6E4576656E745F666B71007E00014C000C636F6C6C656374696F6E494471007E00014C000F636F6C6C6563746F724E756D62657271007E00014C000D64616D61676552656D61726B7371007E00014C00136461746147656E6572616C697A6174696F6E7371007E00014C000E646174654964656E74696669656471007E00014C001264657269766174696F6E4576656E745F666B71007E00014C001B6465726976656446726F6D4D6174657269616C456E74697479494471007E00014C001C6465726976656446726F6D4D6174657269616C456E746974795F666B71007E00014C00116469676974616C53706563696D656E494471007E00014C000A6469736369706C696E6571007E00014C000B646973706F736974696F6E71007E00014C001865766964656E6365466F724F6363757272656E63655F666B71007E00014C000B666565646261636B55524C71007E00014C001468616E646C696E67526571756972656D656E747371007E00014C000D68617A61726452656D61726B7371007E00014C000A68617A6172645479706571007E00014C00186964656E74696669636174696F6E5265666572656E63657371007E00014C00156964656E74696669636174696F6E52656D61726B7371007E00014C00206964656E74696669636174696F6E566572696669636174696F6E53746174757371007E00014C000C6964656E746966696564427971007E00014C000E6964656E7469666965644279494471007E00014C000F6964656E74696669656442795F666B71007E00014C0013696E666F726D6174696F6E5769746868656C6471007E00014C000F696E737469747574696F6E436F646571007E00014C000D696E737469747574696F6E494471007E00014C00186973506172744F664D6174657269616C456E74697479494471007E00014C00196973506172744F664D6174657269616C456E746974795F666B71007E00014C00076C6963656E736571007E00014C00136D6174657269616C4465736372697074696F6E71007E00014C00166D6174657269616C456E7469747943617465676F727971007E00014C00106D6174657269616C456E74697479494471007E00014C00156D6174657269616C456E7469747952656D61726B7371007E00014C00126D6174657269616C456E746974795479706571007E00014C00116D6174657269616C456E746974795F706B71007E00014C00126D6174657269616C50726F706F7274696F6E71007E00014C00126D6174657269616C5265666572656E63657371007E00014C000C6D6174657269616C526F6C6571007E00014C00136D656173757265644D617373496E4772616D7371007E00014C00086D6F64696669656471007E00014C000E6F626A6563745175616E7469747971007E00014C00126F626A6563745175616E746974795479706571007E00014C00136F74686572436174616C6F674E756D6265727371007E00014C00056F776E657271007E00014C00146F776E6572496E737469747574696F6E436F646571007E00014C00126F776E6572496E737469747574696F6E494471007E00014C00086F776E65725F666B71007E00014C000C7072657061726174696F6E7371007E00014C000D70726F76656E616E63655F666B71007E00014C001273616D706C6564466561747572655479706571007E00014C000E736369656E74696669634E616D6571007E00014C0018736369656E74696669634E616D65417574686F727368697071007E00014C0010736369656E74696669634E616D65494471007E00014C00077461786F6E494471007E00014C00097461786F6E52616E6B71007E00014C000A74726561746D656E747371007E00014C00137479706544657369676E6174696F6E5479706571007E00014C000A747970654F665479706571007E00014C000A7479706553746174757371007E00014C000C74797069666965644E616D6571007E00014C000E7573616765506F6C6963795F666B71007E00014C0016766572626174696D4964656E74696669636174696F6E71007E00014C000D766572626174696D4C6162656C71007E00014C000C766572626174696D4D61737371007E00014C000E7665726E6163756C61724E616D6571007E000178707400136173736F63696174656453657175656E63657374000D636174616C6F674E756D626572740014636C617373696669636174696F6E53797374656D74000B636F6C6C6563746564427974000D636F6C6C65637465644279494474000E636F6C6C656374656442795F666B74000E636F6C6C656374696F6E436F6465740012636F6C6C656374696F6E4576656E745F666B74000C636F6C6C656374696F6E494474000F636F6C6C6563746F724E756D62657274000D64616D61676552656D61726B737400136461746147656E6572616C697A6174696F6E7374000E646174654964656E74696669656474001264657269766174696F6E4576656E745F666B74001B6465726976656446726F6D4D6174657269616C456E74697479494474001C6465726976656446726F6D4D6174657269616C456E746974795F666B7400116469676974616C53706563696D656E494474000A6469736369706C696E6574000B646973706F736974696F6E74001865766964656E6365466F724F6363757272656E63655F666B74000B666565646261636B55524C74001468616E646C696E67526571756972656D656E747374000D68617A61726452656D61726B7374000A68617A617264547970657400186964656E74696669636174696F6E5265666572656E6365737400156964656E74696669636174696F6E52656D61726B737400206964656E74696669636174696F6E566572696669636174696F6E53746174757374000C6964656E746966696564427974000E6964656E7469666965644279494474000F6964656E74696669656442795F666B740013696E666F726D6174696F6E5769746868656C6474000F696E737469747574696F6E436F646574000D696E737469747574696F6E49447400186973506172744F664D6174657269616C456E7469747949447400196973506172744F664D6174657269616C456E746974795F666B7400076C6963656E73657400136D6174657269616C4465736372697074696F6E7400166D6174657269616C456E7469747943617465676F72797400106D6174657269616C456E7469747949447400156D6174657269616C456E7469747952656D61726B737400126D6174657269616C456E74697479547970657400116D6174657269616C456E746974795F706B7400126D6174657269616C50726F706F7274696F6E7400126D6174657269616C5265666572656E63657374000C6D6174657269616C526F6C657400136D656173757265644D617373496E4772616D737400086D6F64696669656474000E6F626A6563745175616E746974797400126F626A6563745175616E74697479547970657400136F74686572436174616C6F674E756D626572737400056F776E65727400146F776E6572496E737469747574696F6E436F64657400126F776E6572496E737469747574696F6E49447400086F776E65725F666B74000C7072657061726174696F6E7374000D70726F76656E616E63655F666B74001273616D706C6564466561747572655479706574000E736369656E74696669634E616D65740018736369656E74696669634E616D65417574686F7273686970740010736369656E74696669634E616D6549447400077461786F6E49447400097461786F6E52616E6B74000A74726561746D656E74737400137479706544657369676E6174696F6E5479706574000A747970654F665479706574000A7479706553746174757374000C74797069666965644E616D6574000E7573616765506F6C6963795F666B740016766572626174696D4964656E74696669636174696F6E74000D766572626174696D4C6162656C74000C766572626174696D4D61737374000E7665726E6163756C61724E616D65";

    private DwcDpService service;
    @Mock
    private ElasticSearchRepository elasticSearchRepository;
    @Mock
    private JobRequestComponent jobRequestComponent;
    @Mock
    private S3Repository s3Repository;
    @Mock
    private IndexProperties indexProperties;
    @Mock
    private DatabaseRepository databaseRepository;
    @Mock
    private JobProperties jobProperties;
    @Mock
    private DwcDpProperties dwcDpProperties;
    @Mock
    private Environment environment;
    @Mock
    private SourceSystemRepository sourceSystemRepository;
    @Mock
    private DataPackageComponent dataPackageComponent;
    @Mock
    private S3Properties s3Properties;

    static Stream<JsonNode> jsonProvider() {
        return Stream.of(givenSpecimenJson(), givenSpecimenJsonOther());
    }

    public static JsonNode givenSpecimenJsonOther() {
        return JSON_MAPPER.readTree(
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
                              "dwc:relationshipOfResource": "hasOrganisationID",
                              "dwc:relatedResourceID": "https://ror.org/040ck2b86",
                              "ods:relatedResourceURI": "https://ror.org/040ck2b86",
                              "dwc:relationshipEstablishedDate": "2025-05-07T14:16:46.170Z"
                            },
                            {
                              "@type": "ods:EntityRelationship",
                              "dwc:relationshipOfResource": "hasSourceSystemID",
                              "dwc:relatedResourceID": "https://hdl.handle.net/TEST/Z1M-8WG-DCD",
                              "ods:relatedResourceURI": "https://hdl.handle.net/TEST/Z1M-8WG-DCD",
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
                              "@id": "https://some-identifier.org",
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
                                  "@id": "https://orcid.org/0000-0002-5669-2769",
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
                              "@id": "https://doi.org/10.5281/zenodo.14383054",
                              "@type": "ods:Event",
                              "dwc:eventType": "Collecting Event",
                              "dwc:fieldNumber": "KAJ2-16.11.18",
                              "dwc:eventDate": "2018-11-16",
                              "ods:hasAgents": [
                                {
                                  "@id": "https://orcid.org/0000-0002-5669-2769",
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
                                "dwc:locationID": "https://doi.org/10.5281/zenodo.14383054",
                                "@type": "ods:Location",
                                "dwc:continent": "Oceania",
                                "dwc:country": "Papua New Guinea",
                                "dwc:locality": "Yawan Village, Huon Peninsula",
                                "dwc:higherGeography": "Papua New Guinea",
                                "ods:hasGeoreference": {
                                  "@type": "ods:Georeference",
                                  "dwc:decimalLatitude": -6.1325,
                                  "dwc:decimalLongitude": 146.84255,
                                  "dwc:geodeticDatum": "WGS84",
                                  "dwc:coordinateUncertaintyInMeters": 1000
                                }
                              }
                            }
                          ]
                        }
                        """
        );
    }

    @BeforeEach
    void setup() throws Exception {
        configuration.setDirectoryForTemplateLoading(new File("src/main/resources/templates/"));
        var template = configuration.getTemplate("dissco-eml.ftl");
        service = new DwcDpService(elasticSearchRepository, jobRequestComponent, s3Repository,
                indexProperties, databaseRepository, jobProperties, dwcDpProperties, environment,
                sourceSystemRepository, dataPackageComponent, JSON_MAPPER, template, s3Properties);
    }

    @AfterEach
    void tearDown() throws IOException {
        var path = Path.of(TEMP_FILE_NAME);
        if (path.toFile().exists()) {
            Files.delete(path);
        }
    }

    @ParameterizedTest
    @MethodSource("jsonProvider")
    void testHandleMessage(JsonNode specimenNode) throws Exception {
        // Given
        given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
                List.of(specimenNode));
        given(environment.getActiveProfiles()).willReturn(new String[]{"dwc_dp"});
        given(jobProperties.getJobId()).willReturn(JOB_ID);
        given(indexProperties.getTempFileLocation()).willReturn(TEMP_FILE_NAME);
        given(dwcDpProperties.getDbPageSize()).willReturn(10);
        given(s3Repository.uploadResults(any(), eq(JOB_ID), eq(".zip"))).willReturn(DOWNLOAD_LINK);
        given(databaseRepository.getRecords(anyString(), eq(0), eq(10))).willReturn(
                List.of());
        var dbResponse = new ArrayList<byte[]>();
        for (int i = 0; i < 18; i++) {
            dbResponse.add(HexFormat.of().parseHex(HEX));
        }
        given(databaseRepository.getRecords("temp_table_cd5c9ee7_material", 0, 10)).willReturn(
                dbResponse.subList(0, 10));
        given(databaseRepository.getRecords("temp_table_cd5c9ee7_material", 10, 10)).willReturn(
                dbResponse.subList(10, 18));
        given(sourceSystemRepository.getEmlBySourceSystemId(SOURCE_SYSTEM_ID)).willReturn(EML);
        given(dataPackageComponent.formatDataPackage(anyString(), anySet())).willReturn("{}");
        ArgumentCaptor<List<Pair<String, Object>>> recordsCaptor = ArgumentCaptor.forClass(List.class);

        // When
        service.handleMessage(givenJobRequest());

        // Then
        then(elasticSearchRepository).should().shutdown();
        then(jobRequestComponent).should().markAsComplete(givenJobRequest(), DOWNLOAD_LINK);
        then(databaseRepository).should(atLeastOnce())
                .insertRecords(anyString(), recordsCaptor.capture());
    }

    @Test
    void testHandleMessageIsSourceSystem() throws Exception {
        // Given
        var eml = "<eml></dataset><dataset><title>Test Dataset</title></dataset></eml>";
        given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
                List.of(givenSpecimenJson()));
        given(environment.getActiveProfiles()).willReturn(new String[]{"dwc_dp"});
        given(jobProperties.getJobId()).willReturn(JOB_ID);
        given(indexProperties.getTempFileLocation()).willReturn(TEMP_FILE_NAME);
        given(dwcDpProperties.getDbPageSize()).willReturn(10);
        given(s3Repository.uploadResults(any(), eq(JOB_ID), eq(".zip"))).willReturn(DOWNLOAD_LINK);
        given(databaseRepository.getRecords(anyString(), eq(0), eq(10))).willReturn(
                List.of());
        given(sourceSystemRepository.getEmlBySourceSystemId(SOURCE_SYSTEM_ID)).willReturn(eml);
        given(dataPackageComponent.formatDataPackage(eq(eml), anySet())).willReturn("{}");
        var dbResponse = new ArrayList<byte[]>();
        for (int i = 0; i < 18; i++) {
            dbResponse.add(HexFormat.of().parseHex(HEX));
        }
        given(databaseRepository.getRecords("temp_table_cd5c9ee7_material", 0, 10)).willReturn(
                dbResponse.subList(0, 10));
        given(databaseRepository.getRecords("temp_table_cd5c9ee7_material", 10, 10)).willReturn(
                dbResponse.subList(10, 18));

        // When
        service.handleMessage(givenSourceSystemRequest());

        // Then
        then(elasticSearchRepository).should().shutdown();
        then(jobRequestComponent).should().markAsComplete(givenSourceSystemRequest(), DOWNLOAD_LINK);
    }

    @Test
    void testHandleMessageIsSourceSystemNoSourceSystemParam() throws Exception {
        // Given
        given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
                List.of(givenSpecimenJson()));
        given(jobProperties.getJobId()).willReturn(JOB_ID);
        given(indexProperties.getTempFileLocation()).willReturn(TEMP_FILE_NAME);

        // When
        service.handleMessage(givenJobRequest(Boolean.TRUE));

        // Then
        then(elasticSearchRepository).should().shutdown();
        then(sourceSystemRepository).shouldHaveNoInteractions();
        then(s3Repository).shouldHaveNoInteractions();
        then(jobRequestComponent).should()
                .updateJobState(givenJobRequest(Boolean.TRUE), JobStateEndpoint.RUNNING);
        then(jobRequestComponent).should()
                .updateJobState(givenJobRequest(Boolean.TRUE), JobStateEndpoint.FAILED);
    }

    @Test
    void testHandleMessageException() throws Exception {
        // Given
        given(elasticSearchRepository.getTargetObjects(any(), any(), eq(null), any())).willReturn(
                List.of(givenSpecimenJson()));
        given(elasticSearchRepository.getTargetMediaById(List.of("https://doi.org/TEST/Y9H-N1L-J7G",
                "https://doi.org/TEST/WVW-SCM-C9Z"))).willReturn(List.of(givenMediaJson()));
        given(jobProperties.getJobId()).willReturn(JOB_ID);
        given(indexProperties.getTempFileLocation()).willReturn("///src/test/resources/temp.zip");

        // When
        service.handleMessage(givenJobRequest());

        // Then
        then(elasticSearchRepository).should().shutdown();
        then(jobRequestComponent).should().updateJobState(givenJobRequest(), JobStateEndpoint.FAILED);
    }

    @Test
    void testCreateTables() {
        // Given
        given(jobProperties.getJobId()).willReturn(JOB_ID);

        // When
        service.setup();

        // Then
        then(databaseRepository).should(times(23)).createTable(anyString());
    }

    @Test
    void testDestroyTables() {
        // Given
        given(jobProperties.getJobId()).willReturn(JOB_ID);

        // When
        service.cleanup();

        // Then
        then(databaseRepository).should(times(23)).dropTable(anyString());
    }
}
