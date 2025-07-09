package eu.dissco.exportjob.service;

import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.AGENT;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.AGENT_IDENTIFIER;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.CHRONOMETRIC_AGE;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.CHRONOMETRIC_AGE_AGENT;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.EVENT;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.EVENT_AGENT;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.EVENT_ASSERTION;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.GEOLOGICAL_CONTEXT;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.IDENTIFICATION;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.IDENTIFICATION_AGENT;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.IDENTIFICATION_TAXON;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.MATERIAL;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.MATERIAL_ASSERTION;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.MATERIAL_IDENTIFIER;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.MATERIAL_MEDIA;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.MATERIAL_REFERENCE;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.MEDIA;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.OCCURRENCE;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.REFERENCE;
import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.RELATIONSHIP;
import static eu.dissco.exportjob.utils.ExportUtils.EXCLUDE_IDENTIFIERS;
import static eu.dissco.exportjob.utils.ExportUtils.EXCLUDE_RELATIONSHIPS;
import static eu.dissco.exportjob.utils.ExportUtils.convertValueToString;
import static eu.dissco.exportjob.utils.ExportUtils.parseAgentDate;
import static eu.dissco.exportjob.utils.ExportUtils.retrieveCombinedAgentId;
import static eu.dissco.exportjob.utils.ExportUtils.retrieveCombinedAgentName;
import static eu.dissco.exportjob.utils.ExportUtils.retrieveCombinedCitation;
import static eu.dissco.exportjob.utils.ExportUtils.retrieveIdentifier;

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
import eu.dissco.exportjob.domain.dwcdp.DwcDpChronometricAge;
import eu.dissco.exportjob.domain.dwcdp.DwcDpChronometricAgeAgent;
import eu.dissco.exportjob.domain.dwcdp.DwcDpClasses;
import eu.dissco.exportjob.domain.dwcdp.DwcDpEvent;
import eu.dissco.exportjob.domain.dwcdp.DwcDpEventAgent;
import eu.dissco.exportjob.domain.dwcdp.DwcDpEventAssertion;
import eu.dissco.exportjob.domain.dwcdp.DwcDpGeologicalContext;
import eu.dissco.exportjob.domain.dwcdp.DwcDpIdentification;
import eu.dissco.exportjob.domain.dwcdp.DwcDpIdentificationAgent;
import eu.dissco.exportjob.domain.dwcdp.DwcDpMaterialAssertion;
import eu.dissco.exportjob.domain.dwcdp.DwcDpMaterialIdentifier;
import eu.dissco.exportjob.domain.dwcdp.DwcDpMaterialMedia;
import eu.dissco.exportjob.domain.dwcdp.DwcDpMaterialReference;
import eu.dissco.exportjob.domain.dwcdp.DwcDpMedia;
import eu.dissco.exportjob.domain.dwcdp.DwcDpOccurrence;
import eu.dissco.exportjob.domain.dwcdp.DwcDpReference;
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
import eu.dissco.exportjob.schema.Identifier;
import eu.dissco.exportjob.schema.OdsHasRole;
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
import org.springframework.util.DigestUtils;

@Slf4j
@Service
@Profile(Profiles.DWC_DP)
public class DwcDpService extends AbstractExportJobService {

  private final ObjectMapper objectMapper;
  private final DatabaseRepository databaseRepository;
  private final JobProperties jobProperties;
  private final DwcDpProperties dwcDpProperties;
  private final DataPackageComponent dataPackageComponent;

  public DwcDpService(
      ElasticSearchRepository elasticSearchRepository, ExporterBackendClient exporterBackendClient,
      S3Repository s3Repository, IndexProperties indexProperties, ObjectMapper objectMapper,
      DatabaseRepository databaseRepository, JobProperties jobProperties,
      DwcDpProperties dwcDpProperties, Environment environment,
      SourceSystemRepository sourceSystemRepository, DataPackageComponent dataPackageComponent) {
    super(elasticSearchRepository, indexProperties, exporterBackendClient, s3Repository,
        environment, sourceSystemRepository);
    this.objectMapper = objectMapper;
    this.databaseRepository = databaseRepository;
    this.jobProperties = jobProperties;
    this.dwcDpProperties = dwcDpProperties;
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
    tableMap.put(MATERIAL_ASSERTION, new ArrayList<>());
    tableMap.put(EVENT_ASSERTION, new ArrayList<>());
    tableMap.put(MATERIAL_REFERENCE, new ArrayList<>());
    tableMap.put(REFERENCE, new ArrayList<>());
    tableMap.put(GEOLOGICAL_CONTEXT, new ArrayList<>());
    tableMap.put(CHRONOMETRIC_AGE, new ArrayList<>());
    tableMap.put(CHRONOMETRIC_AGE_AGENT, new ArrayList<>());
    return tableMap;
  }

  private static void writeRecordsToFile(DwcDpClasses value, List<byte[]> records, FileSystem fs)
      throws IOException, ClassNotFoundException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
    var path = fs.getPath(value.getFileName());
    try (var writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      var csvWriter = new StatefulBeanToCsvBuilder<>(writer).build();
      for (byte[] byteArray : records) {
        var bais = new ByteArrayInputStream(byteArray);
        var objectis = new ObjectInputStream(bais);
        var object = objectis.readObject();
        csvWriter.write(object);
      }
    }
  }

  private void mapRelationship(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results,
      EntityRelationship odsHasEntityRelationship) {
    var relationship = new DwcDpRelationship();
    relationship.setRelationshipID(odsHasEntityRelationship.getId());
    relationship.setSubjectResourceID(digitalSpecimen.getOdsPhysicalSpecimenID());
    relationship.setSubjectResourceType("MaterialEntity");
    relationship.setRelationshipType(odsHasEntityRelationship.getDwcRelationshipOfResource());
    relationship.setRelatedResourceID(
        odsHasEntityRelationship.getOdsRelatedResourceURI().toString());
    relationship.setRelationshipRemarks(odsHasEntityRelationship.getDwcRelationshipRemarks());
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
      relationship.setRelationshipID(generateHashID(relationship.toString()));
    }
    results.get(RELATIONSHIP).add(Pair.of(relationship.getRelationshipID(), relationship));
  }

  private void mapIdentifier(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results, Identifier identifier) {
    var dwcDpIdentifier = new DwcDpMaterialIdentifier();
    dwcDpIdentifier.setIdentifier(identifier.getDctermsIdentifier());
    dwcDpIdentifier.setMaterialEntityID(digitalSpecimen.getOdsPhysicalSpecimenID());
    dwcDpIdentifier.setIdentifierType(identifier.getDctermsTitle());
    results.get(MATERIAL_IDENTIFIER)
        .add(Pair.of(generateHashID(dwcDpIdentifier.toString()), dwcDpIdentifier));
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
              mapChronometricAge(digitalSpecimen, results, eventId);
              mapIdentifiers(digitalSpecimen, results);
              mapOccurrence(digitalSpecimen, results);
              mapIdentification(digitalSpecimen, results);
              mapRelationships(digitalSpecimen, results);
              mapMaterialMedia(digitalSpecimen, results);
              mapMaterialAssertion(digitalSpecimen, results);
              mapMaterialReference(digitalSpecimen, results);
            }
        );
  }

  private void mapChronometricAge(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results, String eventId) {
    if (digitalSpecimen.getOdsHasChronometricAges() != null
        && !digitalSpecimen.getOdsHasChronometricAges().isEmpty()) {
      for (var odsHasChronometricAge : digitalSpecimen.getOdsHasChronometricAges()) {
        var chronometricAge = new DwcDpChronometricAge();
        chronometricAge.setChronometricAgeID(odsHasChronometricAge.getChronoChronometricAgeID());
        chronometricAge.setEventID(eventId);
        chronometricAge.setVerbatimChronometricAge(
            odsHasChronometricAge.getChronoVerbatimChronometricAge());
        chronometricAge.setChronometricAgeProtocol(
            odsHasChronometricAge.getChronoChronometricAgeProtocol());
        chronometricAge.setUncalibratedChronometricAge(
            odsHasChronometricAge.getChronoUncalibratedChronometricAge());
        chronometricAge.setChronometricAgeConversionProtocol(
            odsHasChronometricAge.getChronoChronometricAgeConversionProtocol());
        chronometricAge.setEarliestChronometricAge(
            convertValueToString(odsHasChronometricAge.getChronoEarliestChronometricAge()));
        chronometricAge.setEarliestChronometricAgeReferenceSystem(
            odsHasChronometricAge.getChronoEarliestChronometricAgeReferenceSystem());
        chronometricAge.setLatestChronometricAge(
            convertValueToString(odsHasChronometricAge.getChronoLatestChronometricAge()));
        chronometricAge.setLatestChronometricAgeReferenceSystem(
            odsHasChronometricAge.getChronoLatestChronometricAgeReferenceSystem());
        chronometricAge.setChronometricAgeUncertaintyInYears(
            odsHasChronometricAge.getChronoChronometricAgeUncertaintyInYears());
        chronometricAge.setChronometricAgeUncertaintyMethod(
            odsHasChronometricAge.getChronoChronometricAgeUncertaintyMethod());
        chronometricAge.setMaterialDated(odsHasChronometricAge.getChronoMaterialDated());
        chronometricAge.setMaterialDatedID(odsHasChronometricAge.getChronoMaterialDatedID());
        chronometricAge.setMaterialDatedRelationship(
            odsHasChronometricAge.getChronoMaterialDatedRelationship());
        chronometricAge.setChronometricAgeDeterminedBy(
            retrieveCombinedAgentName(odsHasChronometricAge.getOdsHasAgents(), null));
        chronometricAge.setChronometricAgeDeterminedByID(
            retrieveCombinedAgentId(odsHasChronometricAge.getOdsHasAgents(), null));
        chronometricAge.setChronometricAgeDeterminedDate(
            odsHasChronometricAge.getChronoChronometricAgeDeterminedDate());
        chronometricAge.setChronometricAgeReferences(
            odsHasChronometricAge.getChronoChronometricAgeReferences());
        chronometricAge.setChronometricAgeRemarks(
            odsHasChronometricAge.getChronoChronometricAgeRemarks());
        if (chronometricAge.getChronometricAgeID() == null) {
          chronometricAge.setChronometricAgeID(generateHashID(chronometricAge.toString()));
        }
        results.get(CHRONOMETRIC_AGE)
            .add(Pair.of(chronometricAge.getChronometricAgeID(), chronometricAge));
        mapChronometricAgeAgents(odsHasChronometricAge.getOdsHasAgents(),
            chronometricAge.getChronometricAgeID(), results);
      }
    }
  }

  private void mapChronometricAgeAgents(List<Agent> odsHasAgents, String chronometricAgeID,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    if (odsHasAgents != null && !odsHasAgents.isEmpty()) {
      for (var odsAgent : odsHasAgents) {
        var agent = mapAgent(odsAgent);
        results.get(AGENT).add(Pair.of(agent.getAgentID(), agent));
        mapChronometricAgeAgentRole(odsAgent, chronometricAgeID, agent.getAgentID(), results);
        mapAgentIdentifier(odsAgent, agent.getAgentID(), results);
      }
    }
  }

  private void mapChronometricAgeAgentRole(Agent odsAgent, String chronometricAgeID, String agentID,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (var odsHasRole : odsAgent.getOdsHasRoles()) {
      var role = new DwcDpChronometricAgeAgent();
      role.setAgentID(agentID);
      role.setChronometricAgeID(chronometricAgeID);
      role.setAgentRole(odsHasRole.getSchemaRoleName());
      role.setAgentRoleOrder(odsHasRole.getSchemaPosition());
      role.setAgentRoleDate(parseAgentDate(odsHasRole));
      results.get(CHRONOMETRIC_AGE_AGENT)
          .add(Pair.of(generateHashID(role.toString()), role));
    }
  }

  private DwcDpAgent mapAgent(Agent odsAgent) {
    var agent = new DwcDpAgent();
    agent.setAgentID(odsAgent.getId());
    if (odsAgent.getType() != null) {
      agent.setAgentType(odsAgent.getType().toString());
    }
    agent.setAgentTypeVocabulary("https://schema.org/agent");
    agent.setPreferredAgentName(odsAgent.getSchemaName());
    if (agent.getAgentID() == null) {
      agent.setAgentID(generateHashID(agent.toString()));
    }
    return agent;
  }


  private void mapMaterialReference(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    if (digitalSpecimen.getOdsHasCitations() != null && !digitalSpecimen.getOdsHasCitations()
        .isEmpty()) {
      for (var citation : digitalSpecimen.getOdsHasCitations()) {
        var reference = new DwcDpReference();
        reference.setReferenceID(citation.getDctermsIdentifier());
        reference.setReferenceType(citation.getDctermsType());
        reference.setBibliographicCitation(citation.getDctermsBibliographicCitation());
        reference.setIssued(citation.getDctermsDate());
        reference.setTitle(citation.getDctermsTitle());
        reference.setPagination(citation.getOdsPageNumber());
        reference.setIsPeerReviewed(citation.getOdsIsPeerReviewed());
        reference.setCreator(retrieveCombinedAgentName(citation.getOdsHasAgents(), "creator"));
        reference.setCreatorID(retrieveCombinedAgentId(citation.getOdsHasAgents(), "creator"));
        reference.setPublisher(retrieveCombinedAgentName(citation.getOdsHasAgents(), "publisher"));
        reference.setPublisherID(
            retrieveCombinedAgentId(citation.getOdsHasAgents(), "publisher"));
        if (reference.getReferenceID() == null) {
          reference.setReferenceID(generateHashID(reference.toString()));
        }
        results.get(REFERENCE).add(Pair.of(reference.getReferenceID(), reference));
        mapToMaterialReference(digitalSpecimen.getOdsPhysicalSpecimenID(),
            reference.getReferenceID(), results);
      }
    }
  }

  private void mapToMaterialReference(String odsPhysicalSpecimenID, String referenceID,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    var materialReference = new DwcDpMaterialReference();
    materialReference.setMaterialEntityID(odsPhysicalSpecimenID);
    materialReference.setReferenceID(referenceID);
    results.get(MATERIAL_REFERENCE)
        .add(Pair.of(generateHashID(materialReference.toString()), materialReference));
  }

  private void mapMaterialAssertion(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    if (digitalSpecimen.getOdsHasAssertions() != null
        && !digitalSpecimen.getOdsHasAssertions().isEmpty()) {
      for (var odsHasAssertion : digitalSpecimen.getOdsHasAssertions()) {
        var assertion = new DwcDpMaterialAssertion();
        assertion.setMaterialEntityID(digitalSpecimen.getOdsPhysicalSpecimenID());
        assertion.setAssertionID(odsHasAssertion.getId());
        assertion.setAssertionType(odsHasAssertion.getDwcMeasurementType());
        assertion.setAssertionTypeIRI(odsHasAssertion.getDwciriMeasurementType());
        assertion.setAssertionMadeDate(odsHasAssertion.getDwcMeasurementDeterminedDate());
        assertion.setAssertionValue(odsHasAssertion.getDwcMeasurementValue());
        assertion.setAssertionValueIRI(odsHasAssertion.getDwciriMeasurementValue());
        assertion.setAssertionUnit(odsHasAssertion.getDwcMeasurementUnit());
        assertion.setAssertionUnitIRI(odsHasAssertion.getDwciriMeasurementUnit());
        assertion.setAssertionBy(
            retrieveCombinedAgentName(odsHasAssertion.getOdsHasAgents(), null));
        assertion.setAssertionByID(
            retrieveCombinedAgentId(odsHasAssertion.getOdsHasAgents(), null));
        assertion.setAssertionProtocols(odsHasAssertion.getDwcMeasurementMethod());
        assertion.setAssertionProtocolID(odsHasAssertion.getDwciriMeasurementMethod());
        assertion.setAssertionReferences(
            retrieveCombinedCitation(odsHasAssertion.getOdsHasCitations()));
        assertion.setAssertionRemarks(odsHasAssertion.getDwcMeasurementRemarks());
        if (assertion.getAssertionID() == null) {
          assertion.setAssertionID(generateHashID(assertion.toString()));
        }
        results.get(MATERIAL_ASSERTION).add(Pair.of(assertion.getAssertionID(), assertion));
      }
    }
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
          dpMedia.setMetadataLanguage(media.getAcMetadataLanguage());
          dpMedia.setMetadataLanguageLiteral(media.getAcMetadataLanguageLiteral());
          dpMedia.setSubtype(media.getAcSubtype());
          dpMedia.setSubtypeLiteral(media.getAcSubtypeLiteral());
          dpMedia.setComments(media.getAcComments());
          dpMedia.setRights(media.getDctermsRights());
          dpMedia.setUsageTerms(media.getXmpRightsUsageTerms());
          dpMedia.setWebStatement(media.getXmpRightsWebStatement());
          dpMedia.setAccessURI(media.getAcAccessURI());
          dpMedia.setFormat(media.getDctermsFormat());
          dpMedia.setSource(media.getDctermsSource());
          dpMedia.setDescription(media.getDctermsDescription());
          dpMedia.setTag(String.join(", ", media.getAcTag()));
          dpMedia.setCreateDate(media.getXmpCreateDate());
          dpMedia.setTimeOfDay(media.getAcTimeOfDay());
          dpMedia.setCaptureDevice(media.getAcCaptureDevice());
          dpMedia.setResourceCreationTechnique(media.getAcResourceCreationTechnique());
          dpMedia.setModified(media.getDctermsModified());
          dpMedia.setLanguage(media.getDctermsLanguage());
          dpMedia.setVariant(media.getAcVariant());
          dpMedia.setVariantLiteral(media.getAcVariantLiteral());
          dpMedia.setVariantDescription(media.getAcVariantDescription());
          dpMedia.setPixelXDimension(convertValueToString(media.getExifPixelXDimension()));
          dpMedia.setPixelYDimension(convertValueToString(media.getExifPixelYDimension()));
          results.get(MEDIA).add(Pair.of(dpMedia.getMediaID(), dpMedia));
        });
  }

  private void mapMaterialMedia(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    digitalSpecimen.getOdsHasEntityRelationships().stream()
        .filter(er -> er.getDwcRelationshipOfResource().equals("hasDigitalMedia")).forEach(
            entityRelationship -> {
              var materialMedia = new DwcDpMaterialMedia();
              materialMedia.setMaterialEntityID(digitalSpecimen.getOdsPhysicalSpecimenID());
              materialMedia.setMediaID(entityRelationship.getOdsRelatedResourceURI().toString());
              results.get(MATERIAL_MEDIA)
                  .add(Pair.of(generateHashID(materialMedia.toString()), materialMedia));
            }
        );
  }

  private void mapRelationships(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {

    digitalSpecimen.getOdsHasEntityRelationships().stream()
        .filter(er -> !EXCLUDE_RELATIONSHIPS.contains(er.getDwcRelationshipOfResource())).forEach(
            odsHasEntityRelationship -> mapRelationship(digitalSpecimen, results,
                odsHasEntityRelationship)
        );
  }

  private void mapIdentification(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (var odsHasIdentification : digitalSpecimen.getOdsHasIdentifications()) {
      var identification = new DwcDpIdentification();
      identification.setIdentificationID(odsHasIdentification.getId());
      identification.setBasedOnMaterialEntityID(digitalSpecimen.getOdsPhysicalSpecimenID());
      identification.setIdentificationType("MaterialEntity");
      identification.setVerbatimIdentification(odsHasIdentification.getDwcVerbatimIdentification());
      identification.setIsAcceptedIdentification(
          odsHasIdentification.getOdsIsVerifiedIdentification());
      identification.setTypeStatus(odsHasIdentification.getDwcTypeStatus());
      identification.setDateIdentified(odsHasIdentification.getDwcDateIdentified());
      identification.setIdentificationRemarks(odsHasIdentification.getDwcIdentificationRemarks());
      if (identification.getIdentificationID() == null) {
        identification.setIdentificationID(generateHashID(identification.toString()));
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
      var agent = mapAgent(taxonAgent);
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
      role.setAgentRoleDate(parseAgentDate(odsHasRole));
      results.get(IDENTIFICATION_AGENT).add(Pair.of(generateHashID(role.toString()), role));
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
    material.setMaterialEntityID(digitalSpecimen.getOdsPhysicalSpecimenID());
    material.setDigitalSpecimenID(digitalSpecimen.getId());
    material.setEventID(eventId);
    material.setInstitutionID(digitalSpecimen.getOdsOrganisationID());
    material.setInstitutionCode(digitalSpecimen.getOdsOrganisationCode());
    material.setOwnerInstitutionCode(digitalSpecimen.getOdsOwnerOrganisationCode());
    material.setCollectionCode(digitalSpecimen.getDwcCollectionCode());
    material.setCollectionID(digitalSpecimen.getDwcCollectionID());
    material.setPreparations(digitalSpecimen.getDwcPreparations());
    material.setDisposition(digitalSpecimen.getDwcDisposition());
    material.setCatalogNumber(
        retrieveIdentifier(digitalSpecimen, List.of("dwc:catalogNumber", "abcd:unitID")));
    material.setRecordNumber(
        retrieveIdentifier(digitalSpecimen, List.of("dwc:recordNumber", "abcd:recordURI")));
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
        dwcDpOccurrence.setOccurrenceID(generateHashID(dwcDpOccurrence.toString()));
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
      dwcDpEvent.setEventID(generateHashID(dwcDpEvent.toString()));
    }
    results.get(EVENT).add(Pair.of(dwcDpEvent.getEventID(), dwcDpEvent));
    mapEventAgent(event, dwcDpEvent.getEventID(), results);
    mapEventAssertion(event, dwcDpEvent.getEventID(), results);
    mapGeologicalContext(event, dwcDpEvent.getEventID(), results);
    return dwcDpEvent.getEventID();
  }

  private void mapGeologicalContext(Event event, String eventID,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    if (event.getOdsHasLocation() != null
        && event.getOdsHasLocation().getOdsHasGeologicalContext() != null) {
      var odsHasGeologicalContext = event.getOdsHasLocation().getOdsHasGeologicalContext();
      var geologicalContext = new DwcDpGeologicalContext();
      geologicalContext.setGeologicalContextID(odsHasGeologicalContext.getId());
      geologicalContext.setEventID(eventID);
      geologicalContext.setEarliestEonOrLowestEonothem(
          odsHasGeologicalContext.getDwcEarliestEonOrLowestEonothem());
      geologicalContext.setLatestEonOrHighestEonothem(
          odsHasGeologicalContext.getDwcLatestEonOrHighestEonothem());
      geologicalContext.setEarliestEraOrLowestErathem(
          odsHasGeologicalContext.getDwcEarliestEraOrLowestErathem());
      geologicalContext.setLatestEraOrHighestErathem(
          odsHasGeologicalContext.getDwcLatestEraOrHighestErathem());
      geologicalContext.setEarliestPeriodOrLowestSystem(
          odsHasGeologicalContext.getDwcEarliestPeriodOrLowestSystem());
      geologicalContext.setLatestPeriodOrHighestSystem(
          odsHasGeologicalContext.getDwcLatestPeriodOrHighestSystem());
      geologicalContext.setEarliestEpochOrLowestSeries(
          odsHasGeologicalContext.getDwcEarliestEpochOrLowestSeries());
      geologicalContext.setLatestEpochOrHighestSeries(
          odsHasGeologicalContext.getDwcLatestEpochOrHighestSeries());
      geologicalContext.setEarliestAgeOrLowestStage(
          odsHasGeologicalContext.getDwcEarliestAgeOrLowestStage());
      geologicalContext.setLatestAgeOrHighestStage(
          odsHasGeologicalContext.getDwcLatestAgeOrHighestStage());
      geologicalContext.setLowestBiostratigraphicZone(
          odsHasGeologicalContext.getDwcLowestBiostratigraphicZone());
      geologicalContext.setHighestBiostratigraphicZone(
          odsHasGeologicalContext.getDwcHighestBiostratigraphicZone());
      geologicalContext.setLithostratigraphicTerms(
          odsHasGeologicalContext.getDwcLithostratigraphicTerms());
      geologicalContext.setGroup(odsHasGeologicalContext.getDwcGroup());
      geologicalContext.setFormation(odsHasGeologicalContext.getDwcFormation());
      geologicalContext.setMember(odsHasGeologicalContext.getDwcMember());
      geologicalContext.setBed(odsHasGeologicalContext.getDwcBed());
      if (geologicalContext.getGeologicalContextID() == null) {
        geologicalContext.setGeologicalContextID(generateHashID(geologicalContext.toString()));
      }
      results.get(GEOLOGICAL_CONTEXT)
          .add(Pair.of(geologicalContext.getGeologicalContextID(), geologicalContext));
    }
  }


  private void mapEventAssertion(
      Event event, String eventId, Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    if (event.getOdsHasAssertions() != null && !event.getOdsHasAssertions().isEmpty()) {
      for (var odsHasAssertion : event.getOdsHasAssertions()) {
        var assertion = new DwcDpEventAssertion();
        assertion.setEventID(eventId);
        assertion.setAssertionID(odsHasAssertion.getId());
        assertion.setAssertionType(odsHasAssertion.getDwcMeasurementType());
        assertion.setAssertionTypeIRI(odsHasAssertion.getDwciriMeasurementType());
        assertion.setAssertionMadeDate(odsHasAssertion.getDwcMeasurementDeterminedDate());
        assertion.setAssertionValue(odsHasAssertion.getDwcMeasurementValue());
        assertion.setAssertionValueIRI(odsHasAssertion.getDwciriMeasurementValue());
        assertion.setAssertionUnit(odsHasAssertion.getDwcMeasurementUnit());
        assertion.setAssertionUnitIRI(odsHasAssertion.getDwciriMeasurementUnit());
        assertion.setAssertionBy(
            retrieveCombinedAgentName(odsHasAssertion.getOdsHasAgents(), null));
        assertion.setAssertionByID(
            retrieveCombinedAgentId(odsHasAssertion.getOdsHasAgents(), null));
        assertion.setAssertionProtocols(odsHasAssertion.getDwcMeasurementMethod());
        assertion.setAssertionProtocolID(odsHasAssertion.getDwciriMeasurementMethod());
        assertion.setAssertionReferences(
            retrieveCombinedCitation(odsHasAssertion.getOdsHasCitations()));
        assertion.setAssertionRemarks(odsHasAssertion.getDwcMeasurementRemarks());
        if (assertion.getAssertionID() == null) {
          assertion.setAssertionID(generateHashID(assertion.toString()));
        }
        results.get(EVENT_ASSERTION).add(Pair.of(assertion.getAssertionID(), assertion));
      }
    }
  }

  private void mapEventAgent(Event event, String eventId,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    for (var agent : event.getOdsHasAgents()) {
      var eventAgent = mapAgent(agent);
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
      role.setAgentRoleDate(parseAgentDate(odsHasRole));
      results.get(EVENT_AGENT).add(Pair.of(generateHashID(role.toString()), role));
    }
  }

  private void mapIdentifiers(DigitalSpecimen digitalSpecimen,
      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
    digitalSpecimen.getOdsHasIdentifiers().stream()
        .filter(id -> !EXCLUDE_IDENTIFIERS.contains(id.getDctermsTitle())).forEach(
            identifier -> mapIdentifier(digitalSpecimen, results, identifier)
        );
  }


  private String generateHashID(String objectString) {
    return DigestUtils.md5DigestAsHex(objectString.getBytes(StandardCharsets.UTF_8));
  }
}
