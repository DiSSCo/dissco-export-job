package eu.dissco.exportjob.utils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.SearchParam;
import eu.dissco.exportjob.domain.TargetType;
import java.util.List;
import java.util.UUID;

public class TestUtils {

  private TestUtils() {
  }

  public static final String DOI_1 = "https://doi.org/10.2055/123-456-789";
  public static final String DOI_2 = "https://doi.org/10.2055/ABC-EFG-HIJ";
  public static final String ORG_1 = "https://ror.org/0566bfb96";
  public static final String ORG_2 = "https://ror.org/040ck2b86";
  public static final String PHYS_ID_1 = "AVES.XYZ";
  public static final String PHYS_ID_2 = "AVES.QRS";
  public static final UUID JOB_ID = UUID.fromString("cd5c9ee7-23b1-4615-993e-9d56d0720213");
  public static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules()
      .setSerializationInclusion(Include.NON_NULL);
  public static final String DOWNLOAD_LINK = "https://aws.download/s3";
  public static final String ORG_FIELD_NAME = "$['ods:organisationID']";
  public static final String ID_FIELD = "dcterms:identifier";
  public static final String PHYS_ID_FIELD = "ods:physicalSpecimenID";
  public static final String TEMP_FILE_NAME = "src/main/resources/tmp.zip";


  public static JobRequest givenJobRequest() {
    return new JobRequest(
        givenSearchParams(),
        TargetType.DIGITAL_SPECIMEN,
        JOB_ID
    );
  }

  public static List<SearchParam> givenSearchParams() {
    return List.of(new SearchParam(
        ORG_FIELD_NAME, ORG_1));
  }

  public static JsonNode givenDigitalSpecimen(){
    return givenDigitalSpecimen(DOI_1, ORG_1, PHYS_ID_1);
  }

  public static JsonNode givenDigitalSpecimen(String doi, String org, String physId){
    return MAPPER.createObjectNode()
        .put(ID_FIELD, doi)
        .put("@id", doi)
        .put("ods:organisationID", org)
        .put(PHYS_ID_FIELD, physId);
  }

  public static JsonNode givenDigitalSpecimenReducedDoiList(){
    return givenDigitalSpecimenReducedDoiList(DOI_1, PHYS_ID_1);
  }

  public static JsonNode givenDigitalSpecimenReducedDoiList(String doi, String physId){
    return MAPPER.createObjectNode()
        .put(ID_FIELD, doi)
        .put(PHYS_ID_FIELD, physId);
  }

  public static List<String> givenTargetFields(){
    return List.of(ID_FIELD, PHYS_ID_FIELD);
  }

  public static JsonNode givenSpecimenJson() throws JsonProcessingException {
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

  public static JsonNode givenMediaJson() throws JsonProcessingException {
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
