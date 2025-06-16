package eu.dissco.exportjob.service;

import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.AGENT;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.AGENT_IDENTIFIER;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.EVENT;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.EVENT_AGENT;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.IDENTIFICATION;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.IDENTIFICATION_AGENT;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.IDENTIFICATION_TAXON;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.MATERIAL;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.MATERIAL_IDENTIFIER;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.MATERIAL_MEDIA;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.MEDIA;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.OCCURRENCE;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.RELATIONSHIP;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import eu.dissco.exportjob.Profiles;
import eu.dissco.exportjob.component.DataPackageComponent;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.dwcdp.DwCDpMaterial;
import eu.dissco.exportjob.domain.dwcdp.DwcDpAgent;
import eu.dissco.exportjob.domain.dwcdp.DwcDpAgentIdentifier;
import eu.dissco.exportjob.domain.dwcdp.DwcDpClasses;
import eu.dissco.exportjob.domain.dwcdp.DwcDpEvent;
import eu.dissco.exportjob.domain.dwcdp.DwcDpEventAgent;
import eu.dissco.exportjob.domain.dwcdp.DwcDpIdentification;
import eu.dissco.exportjob.domain.dwcdp.DwcDpIdentificationAgent;
import eu.dissco.exportjob.domain.dwcdp.DwcDpMaterialIdentifier;
import eu.dissco.exportjob.domain.dwcdp.DwcDpMaterialMedia;
import eu.dissco.exportjob.domain.dwcdp.DwcDpMedia;
import eu.dissco.exportjob.domain.dwcdp.DwcDpOccurrence;
import eu.dissco.exportjob.domain.dwcdp.DwcDpRelationship;
import eu.dissco.exportjob.domain.dwcdp.DwcDpTaxonIdentification;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.properties.DwcDpProperties;
import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.properties.JobProperties;
import eu.dissco.exportjob.repository.DatabaseRepository;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.repository.SourceSystemRepository;
import eu.dissco.exportjob.schema.Agent;
import eu.dissco.exportjob.schema.DigitalMedia;
import eu.dissco.exportjob.schema.DigitalSpecimen;
import eu.dissco.exportjob.schema.EntityRelationship;
import eu.dissco.exportjob.schema.Event;
import eu.dissco.exportjob.schema.Identification;
import eu.dissco.exportjob.schema.OdsHasRole;
import eu.dissco.exportjob.utils.DwcDpUtils;
import eu.dissco.exportjob.web.ExporterBackendClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Profile(Profiles.DWC_DP)
public class DwcDpService extends AbstractExportJobService {

  private final ObjectMapper objectMapper;
  private final DatabaseRepository databaseRepository;
  private final JobProperties jobProperties;
  private final DwcDpProperties dwcDpProperties;
  private final SourceSystemRepository sourceSystemRepository;
  private final DataPackageComponent dataPackageComponent;

  public DwcDpService(
      ElasticSearchRepository elasticSearchRepository, ExporterBackendClient exporterBackendClient,
      S3Repository s3Repository, IndexProperties indexProperties, ObjectMapper objectMapper,
      DatabaseRepository databaseRepository, JobProperties jobProperties,
      DwcDpProperties dwcDpProperties, Environment environment,
      SourceSystemRepository sourceSystemRepository, DataPackageComponent dataPackageComponent) {
    super(elasticSearchRepository, indexProperties, exporterBackendClient, s3Repository,
        environment);
    this.objectMapper = objectMapper;
    this.databaseRepository = databaseRepository;
    this.jobProperties = jobProperties;
    this.dwcDpProperties = dwcDpProperties;
    this.sourceSystemRepository = sourceSystemRepository;
    this.dataPackageComponent = dataPackageComponent;
  }

  private static Map<DwcDpClasses, List<Pair<String, Object>>> getTableMap() {
    var tableMap = new EnumMap<DwcDpClasses, List<Pair<String, Object>>>(DwcDpClasses.class);
    tableMap.put(MATERIAL, new ArrayList<>());
    tableMap.put(MATERIAL_IDENTIFIER, new ArrayList<>());
    tableMap.put(EVENT, new ArrayList<>());
    tableMap.put(OCCURRENCE, new ArrayList<>());
    tableMap.put(IDENTIFICATION, new ArrayList<>());
    tableMap.put(IDENTIFICATION_TAXON, new ArrayList<>());
    tableMap.put(AGENT, new ArrayList<>());
    tableMap.put(AGENT_IDENTIFIER, new ArrayList<>());
    tableMap.put(IDENTIFICATION_AGENT, new ArrayList<>());
    tableMap.put(EVENT_AGENT, new ArrayList<>());
    tableMap.put(RELATIONSHIP, new ArrayList<>());
    tableMap.put(MATERIAL_MEDIA, new ArrayList<>());
    tableMap.put(MEDIA, new ArrayList<>());
    return tableMap;
  }

  private static void writeRecordsToFile(DwcDpClasses value, List<byte[]> records, FileSystem fs)
      throws IOException, ClassNotFoundException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
    var path = fs.getPath(value.getFileName());
    try (var writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND)) {
      var csvWriter = new StatefulBeanToCsvBuilder<>(writer).build();
      for (byte[] byteArray : records) {
        var bais = new ByteArrayInputStream(byteArray);
        var objectis = new ObjectInputStream(bais);
        var object = objectis.readObject();
        csvWriter.write(object);
      }
    }
  }

  @PostConstruct()
  public void setup() {
    for (DwcDpClasses value : DwcDpClasses.values()) {
      var tableName = getTempTableName(value);
      log.info("Creating table {}", tableName);
      databaseRepository.createTable(tableName);
    }
  }

  @PreDestroy
  public void cleanup() {
    for (DwcDpClasses value : DwcDpClasses.values()) {
      var tableName = getTempTableName(value);
      log.info("Cleaning up table {}", tableName);
      databaseRepository.dropTable(tableName);
    }
  }

  private String getTempTableName(DwcDpClasses value) {
    return "temp_table_" + jobProperties.getJobId().toString().substring(0, 8) + "_"
        + value.getClassName().replace("-", "_");
  }

  @Override
  protected void postProcessResults(JobRequest jobRequest) throws FailedProcessingException {
    var zipFile = new File(indexProperties.getTempFileLocation());
    try (var fs = FileSystems.newFileSystem(zipFile.toPath(), Map.of("create", "true"))) {
      var filesContainingRecords = new HashSet<DwcDpClasses>();
      for (DwcDpClasses value : DwcDpClasses.values()) {
        var containsRecords = postProcessDwcDpClass(value, fs);
        if (containsRecords) {
          filesContainingRecords.add(value);
        }
      }
      if (Boolean.TRUE.equals(jobRequest.isSourceSystemJob())) {
        var eml = writeEmlFile(jobRequest, fs);
        writeDataPackageFile(eml, fs, filesContainingRecords);
      }
    } catch (IOException ex) {
      log.error("Failed to create zip file", ex);
      throw new FailedProcessingException("Unable to create zip file");
    }
  }

  private void writeDataPackageFile(String eml, FileSystem fs,
      HashSet<DwcDpClasses> filesContainingRecords) throws IOException, FailedProcessingException {
    var dataPackageString = dataPackageComponent.formatDataPackage(eml, filesContainingRecords);
    var dataPackageFile = fs.getPath("data-package.json");
    Files.writeString(dataPackageFile, dataPackageString, StandardCharsets.UTF_8);
  }

  private String writeEmlFile(JobRequest jobRequest, FileSystem fs)
      throws FailedProcessingException, IOException {
    var sourceSystemOptional = jobRequest.searchParams().stream()
        .filter(param -> param.inputField().equals("ods:sourceSystemID."))
        .findFirst();
    if (sourceSystemOptional.isEmpty()) {
      throw new FailedProcessingException(
          "Is a source system job, but no sourceSystemID provided: " + jobRequest.jobId());
    }
    var sourceSystemId = sourceSystemOptional.get().inputValue();
    log.info("Retrieving EML for source system ID: {}", sourceSystemId);
    var eml = sourceSystemRepository.getEmlBySourceSystemId(sourceSystemId);
    var sourceSystemFile = fs.getPath("eml.xml");
    Files.writeString(sourceSystemFile, eml, StandardCharsets.UTF_8);
    return eml;
  }

  private boolean postProcessDwcDpClass(DwcDpClasses value, FileSystem fs)
      throws FailedProcessingException {
    int start = 0;
    boolean continueLoop = true;
    boolean containsRecords = false;
    while (continueLoop) {
      log.info("Retrieving records from table {}, stating at {} with limit {}", value,
          start, dwcDpProperties.getDbPageSize());
      var tableName = getTempTableName(value);
      List<byte[]> records = databaseRepository.getRecords(tableName, start,
          dwcDpProperties.getDbPageSize());
      if (records != null && !records.isEmpty()) {
        containsRecords = true;
        log.info("Writing {} records to csv: {}", records.size(), value.getFileName());
        try {
          writeRecordsToFile(value, records, fs);
        } catch (IOException | CsvDataTypeMismatchException | CsvRequiredFieldEmptyException |
                 ClassNotFoundException e) {
          log.error("Failed to write records to zipFile", e);
          throw new FailedProcessingException("Failed to write records to zipFile");
        }
        if (records.size() < dwcDpProperties.getDbPageSize()) {
          continueLoop = false;
        } else {
          start += dwcDpProperties.getDbPageSize();
        }
      } else {
        continueLoop = false;
      }
    }
    return containsRecords;
  }

  @Override
  protected void writeHeaderToFile() {
    log.debug("This method is not required for DwC-DP exports");
  }

  @Override
  protected void processSearchResults(List<JsonNode> searchResult) throws IOException {
    var results = getTableMap();
    mapSpecimenToDwcDp(results, searchResult);
    addMediaToDwcDp(results);
    pushResultToTempTables(results);
  }

  private void addMediaToDwcDp(Map<DwcDpClasses, List<Pair<String, Object>>> results)
      throws IOException {
    var mediaList = results.get(MATERIAL_MEDIA).stream().map(Pair::getRight)
        .map(DwcDpMaterialMedia.class::cast).map(
            DwcDpMaterialMedia::getMediaID).toList();
    log.info("Retrieving media for {} media ids", mediaList.size());
    if (mediaList.isEmpty()) {
      return;
    }
    var mediaSearchResult = elasticSearchRepository.getTargetMediaById(mediaList);
    mapMediaToDwcDp(results, mediaSearchResult);
  }

  private void pushResultToTempTables(Map<DwcDpClasses, List<Pair<String, Object>>> results)
      throws IOException {
    log.info("Pushing results to temp tables");
    for (var dwcDpClassesListEntry : results.entrySet()) {
      var tableName = getTempTableName(dwcDpClassesListEntry.getKey());
      if (!dwcDpClassesListEntry.getValue().isEmpty()) {
        databaseRepository.insertRecords(tableName, dwcDpClassesListEntry.getValue());
      }
    }
  }

  @Override
  protected void writeResultsToFile(List<JsonNode> searchResults) {
    throw new UnsupportedOperationException("This method is not supported for DwC-DP exports");
  }

  @Override
  protected List<String> targetFields() {
    return List.of();
  }

  private void mapSpecimenToDwcDp(
      Map<DwcDpClasses, List<Pair<String, Object>>> results, List<JsonNode> searchResult) {
    searchResult.stream().map(json -> objectMapper.convertValue(json, DigitalSpecimen.class))
        .forEach(
            digitalSpecimen -> {
              var eventId = mapEvent(digitalSpecimen, results);
              mapMaterial(digitalSpecimen, results, eventId);
              mapIdentifiers(digitalSpecimen, results);
              mapOccurrence(digitalSpecimen, results);
              mapIdentification(digitalSpecimen, results);
              mapRelationships(digitalSpecimen, results);
              mapMaterialMedia(digitalSpecimen, results);
            }
        );
  }

  private void mapMediaToDwcDp(Map<DwcDpClasses, List<Pair<String, Object>>> results,
      List<JsonNode> searchResult) {
    searchResult.stream().map(json -> objectMapper.convertValue(json, DigitalMedia.class))
        .forEach(media -> {
          var dpMedia = new DwcDpMedia();
          dpMedia.setMediaID(media.getId());
          if (media.getDctermsType() != null) {
            dpMedia.setMediaType(media.getDctermsType().toString());
          }
          dpMedia.setAccessURI(media.getAcAccessURI());
          dpMedia.setWebStatement(media.getXmpRightsWebStatement());
          dpMedia.setFormat(media.getDctermsFormat());
          dpMedia.setRights(media.getDctermsRights());
          dpMedia.setSource(media.getDctermsSource());
          dpMedia.setCreateDate(media.getXmpCreateDate());
          dpMedia.setModified(media.getDctermsModified());
          dpMedia.setMediaLanguage(media.getDctermsLanguage());
          results.get(MEDIA).add(Pair.of(dpMedia.getMediaID(), dpMedia));
        });
  }

  private void mapMaterialMedia(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    digitalSpecimen.getOdsHasEntityRelationships().stream()
        .filter(er -> er.getDwcRelationshipOfResource().equals("hasDigitalMedia")).forEach(
            entityRelationship -> {
              var materialMedia = new DwcDpMaterialMedia();
              materialMedia.setMaterialEntityID(digitalSpecimen.getId());
              materialMedia.setMediaID(entityRelationship.getOdsRelatedResourceURI().toString());
              results.get(MATERIAL_MEDIA)
                  .add(Pair.of(Integer.toString(materialMedia.hashCode()), materialMedia));
            }
        );
  }

  private void mapRelationships(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (EntityRelationship odsHasEntityRelationship : digitalSpecimen.getOdsHasEntityRelationships()) {
      var relationship = new DwcDpRelationship();
      relationship.setRelationshipID(odsHasEntityRelationship.getId());
      relationship.setSubjectResourceID(digitalSpecimen.getId());
      relationship.setSubjectResourceType("MaterialEntity");
      relationship.setRelationshipType(odsHasEntityRelationship.getDwcRelationshipOfResource());
      relationship.setRelatedResourceID(
          odsHasEntityRelationship.getOdsRelatedResourceURI().toString());
      if (!odsHasEntityRelationship.getOdsHasAgents().isEmpty()) {
        relationship.setRelationshipAccordingToID(
            odsHasEntityRelationship.getOdsHasAgents().getFirst().getId());
        relationship.setRelationshipAccordingTo(
            odsHasEntityRelationship.getOdsHasAgents().getFirst().getSchemaName());
      }
      if (odsHasEntityRelationship.getDwcRelationshipEstablishedDate() != null) {
        relationship.setRelationshipEffectiveDate(
            odsHasEntityRelationship.getDwcRelationshipEstablishedDate().toString());
      }
      if (relationship.getRelationshipID() == null) {
        relationship.setRelationshipID(Integer.toString(relationship.hashCode()));
      }
      results.get(RELATIONSHIP).add(Pair.of(relationship.getRelationshipID(), relationship));
    }
  }

  private void mapIdentification(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (var odsHasIdentification : digitalSpecimen.getOdsHasIdentifications()) {
      var identification = new DwcDpIdentification();
      identification.setIdentificationID(odsHasIdentification.getId());
      identification.setBasedOnMaterialEntityID(digitalSpecimen.getId());
      identification.setIdentificationType("MaterialEntity");
      identification.setVerbatimIdentification(odsHasIdentification.getDwcVerbatimIdentification());
      identification.setIsAcceptedIdentification(
          odsHasIdentification.getOdsIsVerifiedIdentification());
      identification.setTypeStatus(odsHasIdentification.getDwcTypeStatus());
      identification.setDateIdentified(odsHasIdentification.getDwcDateIdentified());
      identification.setIdentificationRemarks(odsHasIdentification.getDwcIdentificationRemarks());
      identification.setIdentifiedByID(odsHasIdentification.getDwcIdentificationID());
      if (identification.getIdentificationID() == null) {
        identification.setIdentificationID(Integer.toString(identification.hashCode()));
      }
      results.get(IDENTIFICATION)
          .add(Pair.of(identification.getIdentificationID(), identification));
      mapTaxonIdentification(odsHasIdentification, identification.getIdentificationID(), results);
      mapTaxonAgents(odsHasIdentification, identification.getIdentificationID(), results);
    }
  }

  private void mapTaxonAgents(Identification odsHasIdentification, String identificationId,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (var taxonAgent : odsHasIdentification.getOdsHasAgents()) {
      var agent = new DwcDpAgent();
      agent.setAgentID(taxonAgent.getId());
      if (taxonAgent.getType() != null) {
        agent.setAgentType(taxonAgent.getType().toString());
      }
      agent.setAgentTypeVocabulary("https://schema.org/agent");
      agent.setPreferredAgentName(taxonAgent.getSchemaName());
      if (agent.getAgentID() == null) {
        agent.setAgentID(Integer.toString(agent.hashCode()));
      }
      results.get(AGENT).add(Pair.of(agent.getAgentID(), agent));
      mapTaxonAgentRole(taxonAgent, identificationId, agent.getAgentID(), results);
      mapAgentIdentifier(taxonAgent, agent.getAgentID(), results);
    }
  }

  private void mapAgentIdentifier(Agent taxonAgent, String agentId,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (var identifier : taxonAgent.getOdsHasIdentifiers()) {
      var agentIdentifier = new DwcDpAgentIdentifier();
      agentIdentifier.setIdentifier(identifier.getDctermsIdentifier());
      agentIdentifier.setAgentID(agentId);
      agentIdentifier.setIdentifierType(identifier.getDctermsTitle());
      results.get(AGENT_IDENTIFIER).add(Pair.of(agentIdentifier.getIdentifier(), agentIdentifier));
    }
  }

  private void mapTaxonAgentRole(Agent taxonAgent, String identificationId, String agentId,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (OdsHasRole odsHasRole : taxonAgent.getOdsHasRoles()) {
      var role = new DwcDpIdentificationAgent();
      role.setAgentID(agentId);
      role.setIdentificationID(identificationId);
      role.setAgentRole(odsHasRole.getSchemaRoleName());
      role.setAgentRoleOrder(odsHasRole.getSchemaPosition());
      role.setAgentRoleDate(DwcDpUtils.parseAgentDate(odsHasRole));
      results.get(IDENTIFICATION_AGENT).add(Pair.of(Integer.toString(role.hashCode()), role));
    }
  }

  private void mapTaxonIdentification(Identification identification, String identificationId,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (var taxon : identification.getOdsHasTaxonIdentifications()) {
      var taxonIdentification = new DwcDpTaxonIdentification();
      taxonIdentification.setIdentificationID(identificationId);
      taxonIdentification.setTaxonID(taxon.getDwcTaxonID());
      taxonIdentification.setScientificName(taxon.getDwcScientificName());
      taxonIdentification.setTaxonRank(taxon.getDwcTaxonRank());
      taxonIdentification.setTaxonRemarks(taxon.getDwcTaxonRemarks());
      taxonIdentification.setHigherClassificationName(taxon.getDwcKingdom());
      taxonIdentification.setHigherClassificationRank("dwc:kingdom");
      results.get(IDENTIFICATION_TAXON)
          .add(Pair.of(taxonIdentification.getTaxonID(), taxonIdentification));
    }

  }

  private void mapMaterial(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results, String eventId) {
    var material = new DwCDpMaterial();
    material.setMaterialEntityID(digitalSpecimen.getId());
    material.setEventID(eventId);
    material.setInstitutionID(digitalSpecimen.getOdsOrganisationID());
    material.setInstitutionCode(digitalSpecimen.getOdsOrganisationCode());
    material.setOwnerInstitutionCode(digitalSpecimen.getOdsOwnerOrganisationCode());
    material.setCollectionCode(digitalSpecimen.getDwcCollectionCode());
    material.setCollectionID(digitalSpecimen.getDwcCollectionID());
    material.setPreparations(digitalSpecimen.getDwcPreparations());
    material.setDisposition(digitalSpecimen.getDwcDisposition());
    material.setVerbatimLabel(digitalSpecimen.getDwcVerbatimLabel());
    material.setInformationWithheld(digitalSpecimen.getDwcInformationWithheld());
    material.setDataGeneralizations(digitalSpecimen.getDwcDataGeneralizations());
    results.get(MATERIAL).add(Pair.of(material.getMaterialEntityID(), material));
  }

  private void mapOccurrence(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (var event : digitalSpecimen.getOdsHasEvents()) {
      var dwcDpOccurrence = new DwcDpOccurrence();
      dwcDpOccurrence.setOccurrenceID(event.getId());
      dwcDpOccurrence.setEventID(event.getId());
      dwcDpOccurrence.setOrganismQuantity(digitalSpecimen.getDwcOrganismQuantity());
      dwcDpOccurrence.setOrganismQuantityType(digitalSpecimen.getDwcOrganismQuantityType());
      dwcDpOccurrence.setSex(event.getDwcSex());
      dwcDpOccurrence.setLifeStage(event.getDwcLifeStage());
      dwcDpOccurrence.setReproductiveCondition(event.getDwcReproductiveCondition());
      dwcDpOccurrence.setBehavior(event.getDwcBehavior());
      dwcDpOccurrence.setVitality(event.getDwcVitality());
      dwcDpOccurrence.setEstablishmentMeans(event.getDwcEstablishmentMeans());
      dwcDpOccurrence.setDegreeOfEstablishment(event.getDwcDegreeOfEstablishment());
      dwcDpOccurrence.setPathway(event.getDwcPathway());
      dwcDpOccurrence.setOrganismID(digitalSpecimen.getDwcOrganismID());
      dwcDpOccurrence.setOrganismName(digitalSpecimen.getDwcOrganismName());
      dwcDpOccurrence.setOrganismRemarks(digitalSpecimen.getDwcOrganismRemarks());
      dwcDpOccurrence.setOrganismScope(digitalSpecimen.getDwcOrganismScope());
      if (dwcDpOccurrence.getOccurrenceID() == null) {
        dwcDpOccurrence.setOccurrenceID(Integer.toString(dwcDpOccurrence.hashCode()));
      }
      if (!dwcDpOccurrence.isEmpty()) {
        results.get(OCCURRENCE).add(Pair.of(dwcDpOccurrence.getOccurrenceID(), dwcDpOccurrence));
      }
    }
  }

  private String mapEvent(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    if (digitalSpecimen.getOdsHasEvents().isEmpty()) {
      return null;
    }
    var event = digitalSpecimen.getOdsHasEvents().getFirst();
    var dwcDpEvent = new DwcDpEvent();
    dwcDpEvent.setEventID(event.getId());
    dwcDpEvent.setEventType(event.getDwcEventType());
    dwcDpEvent.setDatasetID(digitalSpecimen.getDwcDatasetID());
    dwcDpEvent.setDatasetName(digitalSpecimen.getDwcDatasetName());
    dwcDpEvent.setFieldNumber(event.getDwcFieldNumber());
    dwcDpEvent.setEventDate(event.getDwcEventDate());
    dwcDpEvent.setEventTime(event.getDwcEventTime());
    dwcDpEvent.setStartDayOfYear(event.getDwcStartDayOfYear());
    dwcDpEvent.setEndDayOfYear(event.getDwcEndDayOfYear());
    dwcDpEvent.setYear(event.getDwcYear());
    dwcDpEvent.setMonth(event.getDwcMonth());
    dwcDpEvent.setDay(event.getDwcDay());
    dwcDpEvent.setVerbatimEventDate(event.getDwcVerbatimEventDate());
    dwcDpEvent.setGeoreferenceVerificationStatus(event.getDwcGeoreferenceVerificationStatus());
    dwcDpEvent.setHabitat(event.getDwcHabitat());
    dwcDpEvent.setFieldNotes(event.getDwcFieldNotes());
    dwcDpEvent.setEventRemarks(event.getDwcEventRemarks());
    if (event.getOdsHasLocation() != null) {
      dwcDpEvent.setVerbatimLocality(event.getOdsHasLocation().getDwcVerbatimLocality());
      dwcDpEvent.setVerbatimElevation(event.getOdsHasLocation().getDwcVerbatimElevation());
      dwcDpEvent.setVerbatimDepth(event.getOdsHasLocation().getDwcVerbatimDepth());
      dwcDpEvent.setLocationID(event.getOdsHasLocation().getId());
      dwcDpEvent.setHigherGeographyID(event.getOdsHasLocation().getDwcHigherGeographyID());
      dwcDpEvent.setHigherGeography(event.getOdsHasLocation().getDwcHigherGeography());
      dwcDpEvent.setContinent(event.getOdsHasLocation().getDwcContinent());
      dwcDpEvent.setWaterBody(event.getOdsHasLocation().getDwcWaterBody());
      dwcDpEvent.setIslandGroup(event.getOdsHasLocation().getDwcIslandGroup());
      dwcDpEvent.setIsland(event.getOdsHasLocation().getDwcIsland());
      dwcDpEvent.setCountryCode(event.getOdsHasLocation().getDwcCountryCode());
      dwcDpEvent.setCountry(event.getOdsHasLocation().getDwcCountry());
      dwcDpEvent.setStateProvince(event.getOdsHasLocation().getDwcStateProvince());
      dwcDpEvent.setCounty(event.getOdsHasLocation().getDwcCounty());
      dwcDpEvent.setMunicipality(event.getOdsHasLocation().getDwcMunicipality());
      dwcDpEvent.setLocality(event.getOdsHasLocation().getDwcLocality());
      dwcDpEvent.setMinimumElevationInMeters(
          event.getOdsHasLocation().getDwcMinimumElevationInMeters());
      dwcDpEvent.setMaximumElevationInMeters(
          event.getOdsHasLocation().getDwcMaximumElevationInMeters());
      dwcDpEvent.setVerticalDatum(event.getOdsHasLocation().getDwcVerticalDatum());
      dwcDpEvent.setMinimumDepthInMeters(event.getOdsHasLocation().getDwcMinimumDepthInMeters());
      dwcDpEvent.setMaximumDepthInMeters(event.getOdsHasLocation().getDwcMaximumDepthInMeters());
      dwcDpEvent.setMinimumDistanceAboveSurfaceInMeters(
          event.getOdsHasLocation().getDwcMinimumDistanceAboveSurfaceInMeters());
      dwcDpEvent.setMaximumDistanceAboveSurfaceInMeters(
          event.getOdsHasLocation().getDwcMaximumDistanceAboveSurfaceInMeters());
      dwcDpEvent.setLocationRemarks(event.getOdsHasLocation().getDwcLocationRemarks());
      if (event.getOdsHasLocation().getOdsHasGeoreference() != null) {
        dwcDpEvent.setDecimalLatitude(
            event.getOdsHasLocation().getOdsHasGeoreference().getDwcDecimalLatitude());
        dwcDpEvent.setDecimalLongitude(
            event.getOdsHasLocation().getOdsHasGeoreference().getDwcDecimalLongitude());
        dwcDpEvent.setGeodeticDatum(
            event.getOdsHasLocation().getOdsHasGeoreference().getDwcGeodeticDatum());
        if (event.getOdsHasLocation().getOdsHasGeoreference()
            .getDwcCoordinateUncertaintyInMeters()
            != null) {
          dwcDpEvent.setCoordinateUncertaintyInMeters(
              event.getOdsHasLocation().getOdsHasGeoreference()
                  .getDwcCoordinateUncertaintyInMeters().intValue());
        }
        dwcDpEvent.setCoordinatePrecision(
            event.getOdsHasLocation().getOdsHasGeoreference().getDwcCoordinatePrecision());
        dwcDpEvent.setPointRadiusSpatialFit(
            event.getOdsHasLocation().getOdsHasGeoreference().getDwcPointRadiusSpatialFit());
        dwcDpEvent.setFootprintWKT(
            event.getOdsHasLocation().getOdsHasGeoreference().getDwcFootprintWKT());
        dwcDpEvent.setFootprintSRS(
            event.getOdsHasLocation().getOdsHasGeoreference().getDwcFootprintSRS());
        if (event.getOdsHasLocation().getOdsHasGeoreference().getDwcFootprintSpatialFit()
            != null) {
          dwcDpEvent.setFootprintSpatialFit(
              event.getOdsHasLocation().getOdsHasGeoreference().getDwcFootprintSpatialFit()
                  .doubleValue());
        }
        dwcDpEvent.setGeoreferenceRemarks(
            event.getOdsHasLocation().getOdsHasGeoreference().getDwcGeoreferenceRemarks());
        dwcDpEvent.setGeoreferenceSources(
            event.getOdsHasLocation().getOdsHasGeoreference().getDwcGeoreferenceSources());
        dwcDpEvent.setGeoreferenceProtocol(
            event.getOdsHasLocation().getOdsHasGeoreference().getDwcGeoreferenceProtocol());
      }
    }
    if (dwcDpEvent.getEventID() == null) {
      dwcDpEvent.setEventID(Integer.toString(dwcDpEvent.hashCode()));
    }
    results.get(EVENT).add(Pair.of(dwcDpEvent.getEventID(), dwcDpEvent));
    mapEventAgent(event, dwcDpEvent.getEventID(), results);
    return dwcDpEvent.getEventID();
  }

  private void mapEventAgent(Event event, String eventId,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (var agent : event.getOdsHasAgents()) {
      var eventAgent = new DwcDpAgent();
      eventAgent.setAgentID(agent.getId());
      if (agent.getType() != null) {
        eventAgent.setAgentType(agent.getType().toString());
      }
      eventAgent.setAgentTypeVocabulary("https://schema.org/agent");
      eventAgent.setPreferredAgentName(agent.getSchemaName());
      if (eventAgent.getAgentID() == null) {
        eventAgent.setAgentID(Integer.toString(eventAgent.hashCode()));
      }
      results.get(AGENT).add(Pair.of(eventAgent.getAgentID(), eventAgent));
      mapEventAgentRole(agent, eventId, eventAgent.getAgentID(), results);
      mapAgentIdentifier(agent, eventAgent.getAgentID(), results);
    }
  }

  private void mapEventAgentRole(Agent agent, String eventId, String agentId,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (OdsHasRole odsHasRole : agent.getOdsHasRoles()) {
      var role = new DwcDpEventAgent();
      role.setAgentID(agentId);
      role.setEventID(eventId);
      role.setAgentRole(odsHasRole.getSchemaRoleName());
      role.setAgentRoleOrder(odsHasRole.getSchemaPosition());
      role.setAgentRoleDate(DwcDpUtils.parseAgentDate(odsHasRole));
      results.get(EVENT_AGENT).add(Pair.of(Integer.toString(role.hashCode()), role));
    }
  }

  private void mapIdentifiers(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (var identifier : digitalSpecimen.getOdsHasIdentifiers()) {
      var dwcDpIdentifier = new DwcDpMaterialIdentifier();
      dwcDpIdentifier.setIdentifier(identifier.getDctermsIdentifier());
      dwcDpIdentifier.setMaterialEntityID(digitalSpecimen.getId());
      dwcDpIdentifier.setIdentifierType(identifier.getDctermsTitle());
      results.get(MATERIAL_IDENTIFIER)
          .add(Pair.of(Integer.toString(dwcDpIdentifier.hashCode()), dwcDpIdentifier));
    }
  }
}
