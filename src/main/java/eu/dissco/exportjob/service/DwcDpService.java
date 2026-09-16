package eu.dissco.exportjob.service;

import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import eu.dissco.exportjob.Profiles;
import eu.dissco.exportjob.component.CsvHeaderStrategy;
import eu.dissco.exportjob.component.DataPackageComponent;
import eu.dissco.exportjob.component.JobRequestComponent;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.domain.dwcdp.*;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.properties.DwcDpProperties;
import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.properties.JobProperties;
import eu.dissco.exportjob.properties.S3Properties;
import eu.dissco.exportjob.repository.DatabaseRepository;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.repository.SourceSystemRepository;
import eu.dissco.exportjob.schema.*;
import freemarker.template.Template;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static eu.dissco.exportjob.domain.dwcdp.DwcDpClasses.*;
import static eu.dissco.exportjob.utils.ExportUtils.*;

@Slf4j
@Service
@Profile(Profiles.DWC_DP)
public class DwcDpService extends AbstractDwcService {

    private final DatabaseRepository databaseRepository;
    private final JobProperties jobProperties;
    private final DwcDpProperties dwcDpProperties;
    private final DataPackageComponent dataPackageComponent;

    private final Set<String> sourceSystemList = new HashSet<>();

    public DwcDpService(
            ElasticSearchRepository elasticSearchRepository, JobRequestComponent jobRequestComponent,
            S3Repository s3Repository, IndexProperties indexProperties,
            DatabaseRepository databaseRepository, JobProperties jobProperties,
            DwcDpProperties dwcDpProperties, Environment environment,
            SourceSystemRepository sourceSystemRepository, DataPackageComponent dataPackageComponent,
            JsonMapper mapper, Template emlTemplate, S3Properties s3Properties) {
        super(elasticSearchRepository, indexProperties, mapper, jobRequestComponent, s3Repository,
                environment, sourceSystemRepository, emlTemplate, s3Properties);
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
        tableMap.put(USAGE_POLICY, new ArrayList<>());
        tableMap.put(MATERIAL_USAGE_POLICY, new ArrayList<>());
        tableMap.put(MEDIA_USAGE_POLICY, new ArrayList<>());
        return tableMap;
    }

    private static void writeRecordsToFile(DwcDpClasses value, List<byte[]> records, FileSystem fs,
                                           boolean skipHeader)
            throws IOException, ClassNotFoundException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
        var path = fs.getPath(value.getFileName());
        try (var writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            var csvWriter = getCsvWriter(writer, value.getClazz(), skipHeader);
            for (byte[] byteArray : records) {
                var bais = new ByteArrayInputStream(byteArray);
                var objectis = new ObjectInputStream(bais);
                var object = objectis.readObject();
                csvWriter.write(object);
            }
        }
    }

    private static StatefulBeanToCsv<Object> getCsvWriter(BufferedWriter writer, Class<?> clazz,
                                                          boolean skipHeader) {
        return new StatefulBeanToCsvBuilder<>(writer).withMappingStrategy(
                new CsvHeaderStrategy<>((Class<Object>) clazz, skipHeader)).build();
    }

    private void mapRelationship(DigitalSpecimen digitalSpecimen,
                                 Map<DwcDpClasses, List<Pair<String, Object>>> results,
                                 EntityRelationship odsHasEntityRelationship) {
        var relationship = new DwcDpRelationship();
        relationship.setResourceRelationshipID(odsHasEntityRelationship.getId());
        relationship.setSubjectResourceID(digitalSpecimen.getOdsPhysicalSpecimenID());
        relationship.setSubjectResourceType("MaterialEntity");
        relationship.setRelationshipType(odsHasEntityRelationship.getDwcRelationshipOfResource());
        relationship.setExternalRelatedResourceID(
                odsHasEntityRelationship.getOdsRelatedResourceURI().toString());
        relationship.setRelationshipRemarks(odsHasEntityRelationship.getDwcRelationshipRemarks());
        if (!odsHasEntityRelationship.getOdsHasAgents().isEmpty()) {
            relationship.setRelationshipAccordingToID(
                    odsHasEntityRelationship.getOdsHasAgents().getFirst().getId());
            relationship.setRelationshipAccordingTo(
                    odsHasEntityRelationship.getOdsHasAgents().getFirst().getSchemaName());
        }
        if (odsHasEntityRelationship.getDwcRelationshipEstablishedDate() != null) {
            relationship.setRelationshipEstablishedDate(
                    odsHasEntityRelationship.getDwcRelationshipEstablishedDate().toString());
        }
        if (relationship.getResourceRelationshipID() == null) {
            relationship.setResourceRelationshipID(generateHashID(relationship.toString()));
        }
        results.get(RELATIONSHIP).add(Pair.of(relationship.getResourceRelationshipID(), relationship));
    }

    private void mapIdentifier(DigitalSpecimen digitalSpecimen,
                               Map<DwcDpClasses, List<Pair<String, Object>>> results, Identifier identifier) {
        var dwcDpIdentifier = new DwcDpMaterialIdentifier();
        dwcDpIdentifier.setIdentifier(identifier.getDctermsIdentifier());
        dwcDpIdentifier.setMaterialEntity_fk(digitalSpecimen.getOdsPhysicalSpecimenID());
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
                var containsRecords = postProcessDwcDpClass(value, fs,
                        filesContainingRecords.contains(value));
                if (containsRecords) {
                    filesContainingRecords.add(value);
                }
            }
            writeMetadata(jobRequest, fs, filesContainingRecords);
        } catch (IOException ex) {
            log.error("Failed to create zip file", ex);
            throw new FailedProcessingException("Unable to create zip file");
        }
    }

    private void writeMetadata(JobRequest jobRequest, FileSystem fs, HashSet<DwcDpClasses> filesContainingRecords) throws FailedProcessingException, IOException {
        if (Boolean.TRUE.equals(jobRequest.isSourceSystemJob())) {
            var eml = writeEmlFile(jobRequest, fs);
            writeDataPackageFile(eml, fs, filesContainingRecords);
        } else {
            var eml = writeDiSSCoEml(jobRequest, fs, sourceSystemList.size());
            for (String sourceSystemId : sourceSystemList) {
                writeEmlFileForSourceSystem(sourceSystemId, fs);
            }
            writeDataPackageFile(eml, fs, filesContainingRecords);
        }
    }

    private void writeDataPackageFile(String eml, FileSystem fs,
                                      HashSet<DwcDpClasses> filesContainingRecords) throws IOException, FailedProcessingException {
        var dataPackageString = dataPackageComponent.formatDataPackage(eml, filesContainingRecords);
        var dataPackageFile = fs.getPath("data-package.json");
        Files.writeString(dataPackageFile, dataPackageString, StandardCharsets.UTF_8);
    }


    private boolean postProcessDwcDpClass(DwcDpClasses value, FileSystem fs, boolean skipHeader)
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
                    writeRecordsToFile(value, records, fs, skipHeader);
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
                        DwcDpMaterialMedia::getMedia_fk).toList();
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
        var digitalSpecimenStream = searchResult.stream().map(json -> mapper.convertValue(json, DigitalSpecimen.class)).toList();
        digitalSpecimenStream
                .forEach(
                        digitalSpecimen -> {
                            var eventId = mapEvent(digitalSpecimen, results);
                            mapMaterial(digitalSpecimen, results, eventId);
                            mapChronometricAge(digitalSpecimen, results, eventId);
                            mapIdentifiers(digitalSpecimen, results);
                            mapOccurrence(digitalSpecimen, results, eventId);
                            mapIdentification(digitalSpecimen, results);
                            mapRelationships(digitalSpecimen, results);
                            mapMaterialMedia(digitalSpecimen, results);
                            mapMaterialAssertion(digitalSpecimen, results);
                            mapMaterialReference(digitalSpecimen, results);
                        }
                );
        digitalSpecimenStream.stream().map(DigitalSpecimen::getOdsSourceSystemID).distinct().forEach(
                sourceSystemList::add);
    }

    private void mapChronometricAge(DigitalSpecimen digitalSpecimen,
                                    Map<DwcDpClasses, List<Pair<String, Object>>> results, String eventId) {
        if (digitalSpecimen.getOdsHasChronometricAges() != null
                && !digitalSpecimen.getOdsHasChronometricAges().isEmpty()) {
            for (var odsHasChronometricAge : digitalSpecimen.getOdsHasChronometricAges()) {
                var chronometricAge = new DwcDpChronometricAge();
                chronometricAge.setChronometricAgeID(odsHasChronometricAge.getChronoChronometricAgeID());
                chronometricAge.setEvent_fk(eventId);
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
                chronometricAge.setChronometricAge_pk(chronometricAge.getChronometricAgeID());
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
            for (int i = 0; i < odsHasAgents.size(); i++) {
                var odsAgent = odsHasAgents.get(i);
                var agent = mapAgent(odsAgent);
                results.get(AGENT).add(Pair.of(agent.getAgentID(), agent));
                mapChronometricAgeAgentRole(odsAgent, chronometricAgeID, agent.getAgentID(), i + 1,
                        results);
                mapAgentIdentifier(odsAgent, agent.getAgentID(), results);
            }
        }
    }

    private void mapChronometricAgeAgentRole(Agent odsAgent, String chronometricAgeID,
                                             String agentID, int agentIndex, Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        for (var odsHasRole : odsAgent.getOdsHasRoles()) {
            var role = new DwcDpChronometricAgeAgent();
            role.setAgent_fk(agentID);
            role.setChronometricAge_fk(chronometricAgeID);
            role.setAgentRole(odsHasRole.getSchemaRoleName());
            role.setAgentRoleOrder(
                    odsHasRole.getSchemaPosition() != null ? odsHasRole.getSchemaPosition() : agentIndex);
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
        agent.setPreferredAgentName(odsAgent.getSchemaName());
        if (agent.getAgentID() == null) {
            agent.setAgentID(generateHashID(agent.toString()));
        }
        agent.setAgent_pk(agent.getAgentID());
        return agent;
    }


    private void mapMaterialReference(DigitalSpecimen digitalSpecimen,
                                      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        if (digitalSpecimen.getOdsHasCitations() != null && !digitalSpecimen.getOdsHasCitations()
                .isEmpty()) {
            for (var citation : digitalSpecimen.getOdsHasCitations()) {
                var reference = new DwcDpBibliographicResource();
                reference.setReferenceID(citation.getDctermsIdentifier());
                reference.setReferenceType(citation.getDctermsType());
                reference.setBibliographicCitation(citation.getDctermsBibliographicCitation());
                reference.setIssued(citation.getDctermsDate());
                reference.setTitle(citation.getDctermsTitle());
                reference.setIssued(citation.getDctermsDate());
                reference.setPages(citation.getOdsPageNumber());
                reference.setPeerReviewStatus(citation.getOdsIsPeerReviewed());
                reference.setAuthor(retrieveCombinedAgentName(citation.getOdsHasAgents(), "creator"));
                reference.setAuthorID(retrieveCombinedAgentId(citation.getOdsHasAgents(), "creator"));
                reference.setPublisher(retrieveCombinedAgentName(citation.getOdsHasAgents(), "publisher"));
                reference.setPublisherID(
                        retrieveCombinedAgentId(citation.getOdsHasAgents(), "publisher"));
                reference.setReferenceRemarks(citation.getDctermsDescription());
                if (reference.getReferenceID() == null) {
                    reference.setReferenceID(generateHashID(reference.toString()));
                }
                reference.setReference_pk(reference.getReferenceID());
                results.get(REFERENCE).add(Pair.of(reference.getReferenceID(), reference));
                mapToMaterialReference(digitalSpecimen.getOdsPhysicalSpecimenID(),
                        reference.getReferenceID(), results);
            }
        }
    }

    private void mapToMaterialReference(String odsPhysicalSpecimenID, String referenceID,
                                        Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        var materialReference = new DwcDpMaterialReference();
        materialReference.setMaterialEntity_fk(odsPhysicalSpecimenID);
        materialReference.setReference_fk(referenceID);
        results.get(MATERIAL_REFERENCE)
                .add(Pair.of(generateHashID(materialReference.toString()), materialReference));
    }

    private void mapMaterialAssertion(DigitalSpecimen digitalSpecimen,
                                      Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        if (digitalSpecimen.getOdsHasAssertions() != null
                && !digitalSpecimen.getOdsHasAssertions().isEmpty()) {
            for (var odsHasAssertion : digitalSpecimen.getOdsHasAssertions()) {
                var assertion = new DwcDpMaterialAssertion();
                assertion.setMaterialEntity_fk(digitalSpecimen.getOdsPhysicalSpecimenID());
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
                assertion.setAssertionProtocol_fk(odsHasAssertion.getDwciriMeasurementMethod());
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
        searchResult.stream().map(json -> mapper.convertValue(json, DigitalMedia.class))
                .forEach(media -> {
                    var dpMedia = new DwcDpMedia();
                    dpMedia.setMediaID(media.getId());
                    dpMedia.setMedia_pk(media.getId());
                    if (media.getDctermsType() != null) {
                        dpMedia.setMediaType(media.getDctermsType().toString());
                    }
                    dpMedia.setDescription(media.getDctermsDescription());
                    dpMedia.setTitle(media.getDctermsTitle());
                    dpMedia.setLanguage(media.getDctermsLanguage());
                    dpMedia.setDescription(media.getDctermsDescription());
                    dpMedia.setMetadataLanguageIRI(media.getAcMetadataLanguage());
                    dpMedia.setMetadataLanguageLiteral(media.getAcMetadataLanguageLiteral());
                    dpMedia.setSubtypeIRI(media.getAcSubtype());
                    dpMedia.setSubtypeLiteral(media.getAcSubtypeLiteral());
                    dpMedia.setComments(media.getAcComments());
                    dpMedia.setAccessURI(media.getAcAccessURI());
                    dpMedia.setFormat(media.getDctermsFormat());
                    dpMedia.setAvailable(media.getDctermsAvailable());
                    dpMedia.setComments(media.getAcComments());
                    dpMedia.setSubjectCategoryVocabulary(media.getAcSubjectCategoryVocabulary());
                    dpMedia.setDescription(media.getDctermsDescription());
                    dpMedia.setTag(String.join(", ", media.getAcTag()));
                    dpMedia.setCreateDate(media.getXmpCreateDate());
                    dpMedia.setSubjectOrientationIRI(media.getAcSubjectOrientation());
                    dpMedia.setSubjectOrientationLiteral(media.getAcSubjectOrientationLiteral());
                    dpMedia.setSubjectPartIRI(media.getAcSubjectPart());
                    dpMedia.setSubjectPartLiteral(media.getAcSubjectPartLiteral());
                    dpMedia.setFrameRate(media.getAcFrameRate());
                    dpMedia.setResourceCreationTechnique(media.getAcResourceCreationTechnique());
                    dpMedia.setTimeOfDay(media.getAcTimeOfDay());
                    dpMedia.setCaptureDevice(media.getAcCaptureDevice());
                    dpMedia.setResourceCreationTechnique(media.getAcResourceCreationTechnique());
                    dpMedia.setModified(media.getDctermsModified());
                    dpMedia.setLanguage(media.getDctermsLanguage());
                    dpMedia.setVariantLiteral(media.getAcVariantLiteral());
                    dpMedia.setVariantIRI(media.getAcVariant());
                    dpMedia.setVariantDescription(media.getAcVariantDescription());
                    dpMedia.setPixelXDimension(media.getExifPixelXDimension());
                    dpMedia.setPixelYDimension(media.getExifPixelYDimension());
                    mapUsagePolicyMedia(media, results);
                    results.get(MEDIA).add(Pair.of(dpMedia.getMediaID(), dpMedia));
                });
    }

    private void mapUsagePolicyMedia(DigitalMedia media,
                                     Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        var usagePolicy = new DwcDpUsagePolicy();
        usagePolicy.setRights(media.getDctermsRights());
        usagePolicy.setUsageTerms(media.getXmpRightsUsageTerms());
        usagePolicy.setWebStatement(media.getXmpRightsWebStatement());
        usagePolicy.setOwner(retrieveCombinedAgentName(media.getOdsHasAgents(), "rights-owner"));
        usagePolicy.setOwnerID(retrieveCombinedAgentId(media.getOdsHasAgents(), "rights-owner"));
        if (usagePolicy.getUsagePolicyID() == null) {
            usagePolicy.setUsagePolicyID(generateHashID(usagePolicy.toString()));
        }
        usagePolicy.setUsagePolicy_pk(usagePolicy.getUsagePolicyID());
        if (!usagePolicy.isEmpty()) {
            results.get(USAGE_POLICY).add(Pair.of(usagePolicy.getUsagePolicyID(), usagePolicy));
            var mediaUsagePolicy = new DwcDpMediaUsagePolicy();
            mediaUsagePolicy.setMedia_fk(media.getId());
            mediaUsagePolicy.setUsagePolicy_fk(usagePolicy.getUsagePolicyID());
            results.get(MEDIA_USAGE_POLICY)
                    .add(Pair.of(generateHashID(mediaUsagePolicy.toString()), mediaUsagePolicy));
        }
    }

    private void mapMaterialMedia(DigitalSpecimen digitalSpecimen,
                                  Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        digitalSpecimen.getOdsHasEntityRelationships().stream()
                .filter(er -> er.getDwcRelationshipOfResource().equals("hasDigitalMedia")).forEach(
                        entityRelationship -> {
                            var materialMedia = new DwcDpMaterialMedia();
                            materialMedia.setMaterialEntity_fk(digitalSpecimen.getOdsPhysicalSpecimenID());
                            materialMedia.setMedia_fk(entityRelationship.getOdsRelatedResourceURI().toString());
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
            identification.setMaterialEntity_fk(digitalSpecimen.getOdsPhysicalSpecimenID());
            identification.setVerbatimIdentification(odsHasIdentification.getDwcVerbatimIdentification());
            identification.setIsAcceptedIdentification(
                    odsHasIdentification.getOdsIsVerifiedIdentification());
            identification.setTypeStatus(odsHasIdentification.getDwcTypeStatus());
            identification.setDateIdentified(odsHasIdentification.getDwcDateIdentified());
            identification.setIdentificationRemarks(odsHasIdentification.getDwcIdentificationRemarks());
            if (identification.getIdentificationID() == null) {
                identification.setIdentificationID(generateHashID(identification.toString()));
            }
            identification.setIdentification_pk(identification.getIdentificationID());
            results.get(IDENTIFICATION)
                    .add(Pair.of(identification.getIdentificationID(), identification));
            mapTaxonIdentification(odsHasIdentification, identification.getIdentificationID(), results);
            mapTaxonAgents(odsHasIdentification, identification.getIdentificationID(), results);
        }
    }

    private void mapTaxonAgents(Identification odsHasIdentification, String identificationId,
                                Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        var taxonAgents = odsHasIdentification.getOdsHasAgents();
        for (int i = 0; i < taxonAgents.size(); i++) {
            var taxonAgent = taxonAgents.get(i);
            var agent = mapAgent(taxonAgent);
            results.get(AGENT).add(Pair.of(agent.getAgentID(), agent));
            mapTaxonAgentRole(taxonAgent, identificationId, agent.getAgentID(), i + 1, results);
            mapAgentIdentifier(taxonAgent, agent.getAgentID(), results);
        }
    }

    private void mapAgentIdentifier(Agent taxonAgent, String agentId,
                                    Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        for (var identifier : taxonAgent.getOdsHasIdentifiers()) {
            var agentIdentifier = new DwcDpAgentIdentifier();
            agentIdentifier.setIdentifier(identifier.getDctermsIdentifier());
            agentIdentifier.setAgent_fk(agentId);
            agentIdentifier.setIdentifierType(identifier.getDctermsTitle());
            results.get(AGENT_IDENTIFIER).add(Pair.of(agentIdentifier.getIdentifier(), agentIdentifier));
        }
    }

    private void mapTaxonAgentRole(Agent taxonAgent, String identificationId, String agentId,
                                   int agentIndex, Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        for (OdsHasRole odsHasRole : taxonAgent.getOdsHasRoles()) {
            var role = new DwcDpIdentificationAgent();
            role.setAgent_fk(agentId);
            role.setIdentification_fk(identificationId);
            role.setAgentRole(odsHasRole.getSchemaRoleName());
            role.setAgentRoleOrder(
                    odsHasRole.getSchemaPosition() != null ? odsHasRole.getSchemaPosition() : agentIndex);
            role.setAgentRoleDate(parseAgentDate(odsHasRole));
            results.get(IDENTIFICATION_AGENT).add(Pair.of(generateHashID(role.toString()), role));
        }
    }

    private void mapTaxonIdentification(Identification identification, String identificationId,
                                        Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        var taxonList = identification.getOdsHasTaxonIdentifications();
        for (int i = 0; i < taxonList.size(); i++) {
            var taxon = taxonList.get(i);
            var taxonIdentification = new DwcDpTaxonIdentification();
            taxonIdentification.setIdentification_fk(identificationId);
            taxonIdentification.setTaxonSortOrder(i + 1);
            taxonIdentification.setTaxonID(taxon.getDwcTaxonID());
            taxonIdentification.setScientificName(taxon.getDwcScientificName());
            taxonIdentification.setScientificNameAuthorship(taxon.getDwcScientificNameAuthorship());
            taxonIdentification.setVernacularName(taxon.getDwcVernacularName());
            taxonIdentification.setTaxonRank(taxon.getDwcTaxonRank());
            taxonIdentification.setKingdom(taxon.getDwcKingdom());
            taxonIdentification.setPhylum(taxon.getDwcPhylum());
            taxonIdentification.setClazz(taxon.getDwcClass());
            taxonIdentification.setOrder(taxon.getDwcOrder());
            taxonIdentification.setFamily(taxon.getDwcFamily());
            taxonIdentification.setSubfamily(taxon.getDwcSubfamily());
            taxonIdentification.setGenus(taxon.getDwcGenus());
            taxonIdentification.setGenericName(taxon.getDwcGenericName());
            taxonIdentification.setSubgenus(taxon.getDwcSubgenus());
            taxonIdentification.setInfragenericEpithet(taxon.getDwcInfragenericEpithet());
            taxonIdentification.setSpecificEpithet(taxon.getDwcSpecificEpithet());
            taxonIdentification.setInfraspecificEpithet(taxon.getDwcInfraspecificEpithet());
            taxonIdentification.setCultivarEpithet(taxon.getDwcCultivarEpithet());
            taxonIdentification.setNomenclaturalCode(taxon.getDwcNomenclaturalCode());
            taxonIdentification.setNomenclaturalStatus(taxon.getDwcNomenclaturalStatus());
            taxonIdentification.setNamePublishedInYear(taxon.getDwcNamePublishedInYear());
            results.get(IDENTIFICATION_TAXON)
                    .add(Pair.of(taxonIdentification.getTaxonID(), taxonIdentification));
        }

    }

    private void mapMaterial(DigitalSpecimen digitalSpecimen,
                             Map<DwcDpClasses, List<Pair<String, Object>>> results, String eventId) {
        var material = new DwCDpMaterial();
        material.setMaterialEntityID(digitalSpecimen.getOdsPhysicalSpecimenID());
        material.setMaterialEntity_pk(digitalSpecimen.getOdsPhysicalSpecimenID());
        material.setDigitalSpecimenID(digitalSpecimen.getId());
        material.setCollectionEvent_fk(eventId);
        material.setDiscipline(digitalSpecimen.getOdsTopicDiscipline().value());
        material.setInstitutionID(digitalSpecimen.getOdsOrganisationID());
        material.setInstitutionCode(digitalSpecimen.getOdsOrganisationCode());
        material.setOwnerInstitutionCode(digitalSpecimen.getOdsOwnerOrganisationCode());
        material.setCollectionCode(digitalSpecimen.getDwcCollectionCode());
        material.setCollectionID(digitalSpecimen.getDwcCollectionID());
        material.setPreparations(digitalSpecimen.getDwcPreparations());
        material.setDisposition(digitalSpecimen.getDwcDisposition());
        material.setCatalogNumber(
                retrieveIdentifier(digitalSpecimen, List.of("dwc:catalogNumber", "abcd:unitID")));
        material.setCollectorNumber(
                retrieveIdentifier(digitalSpecimen, List.of("dwc:recordNumber", "abcd:recordURI")));
        material.setVerbatimLabel(digitalSpecimen.getDwcVerbatimLabel());
        material.setInformationWithheld(digitalSpecimen.getDwcInformationWithheld());
        material.setDataGeneralizations(digitalSpecimen.getDwcDataGeneralizations());
        material.setFeedbackURL(digitalSpecimen.getDctermsIdentifier());
        mapUsagePolicySpecimen(digitalSpecimen, results);
        results.get(MATERIAL).add(Pair.of(material.getMaterialEntityID(), material));
    }

    private void mapUsagePolicySpecimen(DigitalSpecimen digitalSpecimen,
                                        Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        var usagePolicy = new DwcDpUsagePolicy();
        usagePolicy.setLicense(digitalSpecimen.getDctermsLicense());
        usagePolicy.setAccessRights(digitalSpecimen.getDctermsAccessRights());
        usagePolicy.setRightsHolder(digitalSpecimen.getDctermsRightsHolder());
        if (usagePolicy.getUsagePolicyID() == null) {
            usagePolicy.setUsagePolicyID(generateHashID(usagePolicy.toString()));
        }
        usagePolicy.setUsagePolicy_pk(usagePolicy.getUsagePolicyID());
        if (!usagePolicy.isEmpty()) {
            results.get(USAGE_POLICY).add(Pair.of(usagePolicy.getUsagePolicyID(), usagePolicy));
            var materialUsagePolicy = new DwcDpMaterialUsagePolicy();
            materialUsagePolicy.setMaterialEntity_fk(digitalSpecimen.getOdsPhysicalSpecimenID());
            materialUsagePolicy.setUsagePolicy_fk(usagePolicy.getUsagePolicyID());
            results.get(MATERIAL_USAGE_POLICY)
                    .add(Pair.of(generateHashID(materialUsagePolicy.toString()), materialUsagePolicy));
        }
    }


    private void mapOccurrence(DigitalSpecimen digitalSpecimen,
                               Map<DwcDpClasses, List<Pair<String, Object>>> results, String eventId) {
        for (var event : digitalSpecimen.getOdsHasEvents()) {
            var dwcDpOccurrence = new DwcDpOccurrence();
            dwcDpOccurrence.setOccurrenceID(event.getId());
            dwcDpOccurrence.setEvent_fk(eventId);
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
            dwcDpOccurrence.setOccurrence_pk(dwcDpOccurrence.getOccurrenceID());
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
        dwcDpEvent.setEventCategory("material collection");
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
                                    .getDwcCoordinateUncertaintyInMeters());
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
        dwcDpEvent.setEvent_pk(dwcDpEvent.getEventID());
        results.get(EVENT).add(Pair.of(dwcDpEvent.getEventID(), dwcDpEvent));
        mapEventAgent(event, dwcDpEvent.getEventID(), results);
        mapEventAssertion(event, dwcDpEvent.getEventID(), results);
        dwcDpEvent.setGeologicalContextID(mapGeologicalContext(event, results));
        return dwcDpEvent.getEventID();
    }

    private String mapGeologicalContext(Event event,
                                        Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        if (event.getOdsHasLocation() != null
                && event.getOdsHasLocation().getOdsHasGeologicalContext() != null) {
            var odsHasGeologicalContext = event.getOdsHasLocation().getOdsHasGeologicalContext();
            var geologicalContext = new DwcDpGeologicalContext();
            geologicalContext.setGeologicalContextID(odsHasGeologicalContext.getId());
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
            geologicalContext.setGeologicalContext_pk(geologicalContext.getGeologicalContextID());
            results.get(GEOLOGICAL_CONTEXT)
                    .add(Pair.of(geologicalContext.getGeologicalContextID(), geologicalContext));
            return geologicalContext.getGeologicalContextID();
        }
        return null;
    }


    private void mapEventAssertion(
            Event event, String eventId, Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        if (event.getOdsHasAssertions() != null && !event.getOdsHasAssertions().isEmpty()) {
            for (var odsHasAssertion : event.getOdsHasAssertions()) {
                var assertion = new DwcDpEventAssertion();
                assertion.setEvent_fk(eventId);
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
                assertion.setAssertionProtocol_fk(odsHasAssertion.getDwciriMeasurementMethod());
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
        var agents = event.getOdsHasAgents();
        for (int i = 0; i < agents.size(); i++) {
            var agent = agents.get(i);
            var eventAgent = mapAgent(agent);
            results.get(AGENT).add(Pair.of(eventAgent.getAgentID(), eventAgent));
            mapEventAgentRole(agent, eventId, eventAgent.getAgentID(), i + 1, results);
            mapAgentIdentifier(agent, eventAgent.getAgentID(), results);
        }
    }

    private void mapEventAgentRole(Agent agent, String eventId, String agentId, int agentIndex,
                                   Map<DwcDpClasses, List<Pair<String, Object>>> results) {
        for (OdsHasRole odsHasRole : agent.getOdsHasRoles()) {
            var role = new DwcDpEventAgent();
            role.setAgent_fk(agentId);
            role.setEvent_fk(eventId);
            role.setAgentRole(odsHasRole.getSchemaRoleName());
            role.setAgentRoleOrder(
                    odsHasRole.getSchemaPosition() != null ? odsHasRole.getSchemaPosition() : agentIndex);
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
