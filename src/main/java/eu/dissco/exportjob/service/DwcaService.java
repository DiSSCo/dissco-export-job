package eu.dissco.exportjob.service;

import static eu.dissco.exportjob.utils.ExportUtils.EXCLUDE_IDENTIFIERS;
import static eu.dissco.exportjob.utils.ExportUtils.EXCLUDE_RELATIONSHIPS;
import static eu.dissco.exportjob.utils.ExportUtils.convertValueToString;
import static eu.dissco.exportjob.utils.ExportUtils.retrieveCombinedAgentId;
import static eu.dissco.exportjob.utils.ExportUtils.retrieveCombinedAgentName;
import static eu.dissco.exportjob.utils.ExportUtils.retrieveIdentifier;
import static eu.dissco.exportjob.utils.ExportUtils.retrieveTerm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.exportjob.Profiles;
import eu.dissco.exportjob.component.DwcaZipWriter;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.repository.SourceSystemRepository;
import eu.dissco.exportjob.schema.Agent;
import eu.dissco.exportjob.schema.Assertion;
import eu.dissco.exportjob.schema.Citation;
import eu.dissco.exportjob.schema.DigitalMedia;
import eu.dissco.exportjob.schema.DigitalSpecimen;
import eu.dissco.exportjob.schema.EntityRelationship;
import eu.dissco.exportjob.schema.Identification;
import eu.dissco.exportjob.schema.Identifier;
import eu.dissco.exportjob.schema.TaxonIdentification;
import eu.dissco.exportjob.web.ExporterBackendClient;
import freemarker.template.TemplateException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.gbif.dwc.terms.AcTerm;
import org.gbif.dwc.terms.ChronoTerm;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.DwcaTerm;
import org.gbif.dwc.terms.ExifTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.ObisTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.XmpRightsTerm;
import org.gbif.dwc.terms.XmpTerm;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Profile(Profiles.DWCA)
public class DwcaService extends AbstractExportJobService {

  public static final Pair<Predicate<DigitalSpecimen>, Function<DigitalSpecimen, Object>> EVENT_FUNCTIONS =
      Pair.of(ds -> ds.getOdsHasEvents() == null || ds.getOdsHasEvents().isEmpty(),
          ds -> ds.getOdsHasEvents().getFirst());
  public static final Pair<Predicate<DigitalSpecimen>, Function<DigitalSpecimen, Object>> LOCATION_FUNCTIONS =
      Pair.of(EVENT_FUNCTIONS.getLeft().or(
              ds -> ds.getOdsHasEvents().getFirst().getOdsHasLocation() == null),
          ds -> ds.getOdsHasEvents().getFirst().getOdsHasLocation());
  public static final Pair<Predicate<DigitalSpecimen>, Function<DigitalSpecimen, Object>> GEOLOGICAL_CONTEXT_FUNCTIONS =
      Pair.of(LOCATION_FUNCTIONS.getLeft()
              .or(ds -> ds.getOdsHasEvents().getFirst().getOdsHasLocation().getOdsHasGeologicalContext()
                  == null),
          ds -> ds.getOdsHasEvents().getFirst().getOdsHasLocation().getOdsHasGeologicalContext());
  public static final Pair<Predicate<DigitalSpecimen>, Function<DigitalSpecimen, Object>> GEOREFERENCE_FUNCTIONS =
      Pair.of(LOCATION_FUNCTIONS.getLeft().or(ds ->
              ds.getOdsHasEvents().getFirst().getOdsHasLocation().getOdsHasGeoreference() == null),
          ds -> ds.getOdsHasEvents().getFirst().getOdsHasLocation().getOdsHasGeoreference());

  private static final String OCCURRENCE_ID = "dwc:occurrenceID";
  private static final String UNIT_GUID = "abcd:unitGUID";
  private static final String UNIT_ID = "abcd:unitID";


  private final ObjectMapper objectMapper;
  private final DwcaZipWriter dwcaZipWriter;

  public DwcaService(
      ElasticSearchRepository elasticSearchRepository, ExporterBackendClient exporterBackendClient,
      S3Repository s3Repository, IndexProperties indexProperties, ObjectMapper objectMapper,
      Environment environment, SourceSystemRepository sourceSystemRepository,
      DwcaZipWriter dwcaZipWriter) {
    super(elasticSearchRepository, indexProperties, exporterBackendClient, s3Repository,
        environment, sourceSystemRepository);
    this.objectMapper = objectMapper;
    this.dwcaZipWriter = dwcaZipWriter;
  }

  private static String getStringValue(Object object) {
    if (object == null) {
      return null;
    }
    return String.valueOf(object);
  }

  @Override
  protected void writeHeaderToFile() throws IOException {
    log.debug("This method is not required for DWCA exports");
  }

  @Override
  protected void postProcessResults(JobRequest jobRequest)
      throws IOException, FailedProcessingException {
    if (Boolean.TRUE.equals(jobRequest.isSourceSystemJob())) {
      writeEmlFile(jobRequest, dwcaZipWriter.getFileSystem());
    }
    try {
      dwcaZipWriter.close();
    } catch (TemplateException e) {
      throw new FailedProcessingException("Failed to create the metadata file", e);
    }
  }

  @Override
  protected void processSearchResults(List<JsonNode> searchResults)
      throws IOException, FailedProcessingException {
    var digitalSpecimenList = searchResults.stream()
        .map(json -> objectMapper.convertValue(json, DigitalSpecimen.class)).toList();
    var digitalMediaList = elasticSearchRepository.getTargetMediaById(
            getMediaIds(digitalSpecimenList)).stream()
        .map(json -> objectMapper.convertValue(json, DigitalMedia.class)).toList();
    var specimenToDigitalMediaMapping = createSpecimenToMediaMapping(digitalSpecimenList,
        digitalMediaList);
    var mappedResult = mapToDwcaRecords(digitalSpecimenList, specimenToDigitalMediaMapping);
    dwcaZipWriter.writeRecords(mappedResult);
  }

  private Map<Term, List<List<Pair<Term, String>>>> mapToDwcaRecords(
      List<DigitalSpecimen> digitalSpecimenList,
      Map<String, List<DigitalMedia>> specimenToDigitalMediaMapping)
      throws FailedProcessingException {
    var mappedList = new HashMap<Term, List<List<Pair<Term, String>>>>();
    var occurrenceList = new ArrayList<List<Pair<Term, String>>>();
    var identificationList = new ArrayList<List<Pair<Term, String>>>();
    var referenceList = new ArrayList<List<Pair<Term, String>>>();
    var identifierList = new ArrayList<List<Pair<Term, String>>>();
    var relationshipList = new ArrayList<List<Pair<Term, String>>>();
    var digitalMediaList = new ArrayList<List<Pair<Term, String>>>();
    var assertionList = new ArrayList<List<Pair<Term, String>>>();
    var chronometricAgeList = new ArrayList<List<Pair<Term, String>>>();
    for (var digitalSpecimen : digitalSpecimenList) {
      addOccurrence(digitalSpecimen, occurrenceList);
      addIdentifications(digitalSpecimen, identificationList, referenceList);
      addIdentifiers(digitalSpecimen, identifierList);
      addRelationships(digitalSpecimen, relationshipList);
      addReference(digitalSpecimen.getOdsHasCitations(), digitalSpecimen.getId(), referenceList);
      addAnnotation(digitalSpecimen, assertionList);
      addChronometricAge(digitalSpecimen, chronometricAgeList);
      var media = specimenToDigitalMediaMapping.get(digitalSpecimen.getId());
      if (media != null && !media.isEmpty()) {
        addDigitalMedia(specimenToDigitalMediaMapping.get(digitalSpecimen.getId()),
            digitalSpecimen.getId(), digitalMediaList);
      }
    }
    mappedList.put(DwcTerm.Occurrence, occurrenceList);
    mappedList.put(DwcTerm.Identification, identificationList);
    mappedList.put(GbifTerm.Identifier, identifierList);
    mappedList.put(GbifTerm.Reference, referenceList);
    mappedList.put(DwcTerm.ResourceRelationship, relationshipList);
    mappedList.put(AcTerm.Multimedia, digitalMediaList);
    mappedList.put(ChronoTerm.ChronometricAge, chronometricAgeList);
    return mappedList;
  }

  private void addChronometricAge(DigitalSpecimen digitalSpecimen,
      ArrayList<List<Pair<Term, String>>> chronometricAgeList) {
    if (digitalSpecimen.getOdsHasChronometricAges() != null
        && !digitalSpecimen.getOdsHasChronometricAges().isEmpty()) {
      for (var chronometricAge : digitalSpecimen.getOdsHasChronometricAges()) {
        var chronometricAgeRecord = new ArrayList<Pair<Term, String>>();
        chronometricAgeRecord.add(Pair.of(DwcaTerm.ID, digitalSpecimen.getId()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.chronometricAgeID,
            chronometricAge.getChronoChronometricAgeID()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.verbatimChronometricAge,
            chronometricAge.getChronoVerbatimChronometricAge()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.chronometricAgeProtocol,
            chronometricAge.getChronoChronometricAgeProtocol()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.uncalibratedChronometricAge,
            chronometricAge.getChronoUncalibratedChronometricAge()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.chronometricAgeConversionProtocol,
            chronometricAge.getChronoChronometricAgeConversionProtocol()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.earliestChronometricAge,
            convertValueToString(chronometricAge.getChronoEarliestChronometricAge())));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.earliestChronometricAgeReferenceSystem,
            chronometricAge.getChronoEarliestChronometricAgeReferenceSystem()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.latestChronometricAge,
            convertValueToString(chronometricAge.getChronoLatestChronometricAge())));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.latestChronometricAgeReferenceSystem,
            chronometricAge.getChronoLatestChronometricAgeReferenceSystem()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.chronometricAgeUncertaintyInYears,
            getStringValue(chronometricAge.getChronoChronometricAgeUncertaintyInYears())));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.chronometricAgeUncertaintyMethod,
            chronometricAge.getChronoChronometricAgeUncertaintyMethod()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.materialDated,
            chronometricAge.getChronoMaterialDated()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.materialDatedID,
            chronometricAge.getChronoMaterialDatedID()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.materialDatedRelationship,
            chronometricAge.getChronoMaterialDatedRelationship()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.chronometricAgeDeterminedBy, retrieveCombinedAgentName(chronometricAge.getOdsHasAgents(), null)));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.chronometricAgeDeterminedDate,
            chronometricAge.getChronoChronometricAgeDeterminedDate()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.chronometricAgeReferences,
            chronometricAge.getChronoChronometricAgeReferences()));
        chronometricAgeRecord.add(Pair.of(ChronoTerm.chronometricAgeRemarks,
            chronometricAge.getChronoChronometricAgeRemarks()));
        chronometricAgeList.add(chronometricAgeRecord);
      }
    }
  }

  private void mapEvent(DigitalSpecimen digitalSpecimen,
      ArrayList<Pair<Term, String>> occurrenceRecord)
      throws FailedProcessingException {
    occurrenceRecord.add(Pair.of(DwcTerm.sex,
        retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcSex")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.lifeStage,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcLifeStage")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.reproductiveCondition,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS,
                "getDwcReproductiveCondition")));
    occurrenceRecord.add(Pair.of(DwcTerm.caste,
        retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcCaste")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.behavior,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcBehavior")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.vitality,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcVitality")));
    occurrenceRecord.add(Pair.of(DwcTerm.establishmentMeans,
        retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcEstablishmentMeans")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.degreeOfEstablishment,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS,
                "getDwcDegreeOfEstablishment")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.pathway,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcPathway")));
    occurrenceRecord.add(Pair.of(DwcTerm.georeferenceVerificationStatus,
        retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS,
            "getDwcGeoreferenceVerificationStatus")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.eventType,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcEventType")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.fieldNumber,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcFieldNumber")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.eventDate,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcEventDate")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.eventTime,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcEventTime")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.startDayOfYear,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcStartDayOfYear")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.endDayOfYear,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcEndDayOfYear")));
    occurrenceRecord.add(Pair.of(DwcTerm.year,
        retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcYear")));
    occurrenceRecord.add(Pair.of(DwcTerm.month,
        retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcMonth")));
    occurrenceRecord.add(Pair.of(DwcTerm.day,
        retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcDay")));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimEventDate,
        retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcVerbatimEventDate")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.habitat,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcHabitat")));
    occurrenceRecord.add(Pair.of(DwcTerm.samplingProtocol,
        retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcSamplingProtocol")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.sampleSizeValue,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcSampleSizeValue")));
    occurrenceRecord.add(Pair.of(DwcTerm.sampleSizeUnit,
        retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcSampleSizeUnit")));
    occurrenceRecord.add(Pair.of(DwcTerm.samplingEffort,
        retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcSamplingEffort")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.fieldNotes,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcFieldNotes")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.eventRemarks,
            retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, "getDwcEventRemarks")));
    occurrenceRecord.add(Pair.of(DwcTerm.eventID, digitalSpecimen.getId()));
  }

  private void mapLocation(DigitalSpecimen digitalSpecimen,
      ArrayList<Pair<Term, String>> occurrenceRecord) throws FailedProcessingException {
    occurrenceRecord.add(Pair.of(DwcTerm.locationID,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getId")));
    occurrenceRecord.add(Pair.of(DwcTerm.higherGeography,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcHigherGeography")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.higherGeographyID,
            retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS,
                "getDwcHigherGeographyID")));
    occurrenceRecord.add(Pair.of(DwcTerm.continent,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcContinent")));
    occurrenceRecord.add(Pair.of(DwcTerm.waterBody,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcWaterBody")));
    occurrenceRecord.add(Pair.of(DwcTerm.island,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcIsland")));
    occurrenceRecord.add(Pair.of(DwcTerm.islandGroup,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcIslandGroup")));
    occurrenceRecord.add(Pair.of(DwcTerm.country,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcCountry")));
    occurrenceRecord.add(Pair.of(DwcTerm.countryCode,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcCountryCode")));
    occurrenceRecord.add(Pair.of(DwcTerm.stateProvince,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcStateProvince")));
    occurrenceRecord.add(Pair.of(DwcTerm.county,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcCounty")));
    occurrenceRecord.add(Pair.of(DwcTerm.municipality,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcMunicipality")));
    occurrenceRecord.add(Pair.of(DwcTerm.locality,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcLocality")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.verbatimLocality,
            retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS,
                "getDwcVerbatimLocality")));
    occurrenceRecord.add(Pair.of(DwcTerm.minimumElevationInMeters,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS,
            "getDwcMinimumElevationInMeters")));
    occurrenceRecord.add(Pair.of(DwcTerm.maximumElevationInMeters,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS,
            "getDwcMaximumElevationInMeters")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.verbatimElevation,
            retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS,
                "getDwcVerbatimElevation")));
    occurrenceRecord.add(Pair.of(DwcTerm.minimumDepthInMeters,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS,
            "getDwcMinimumDepthInMeters")));
    occurrenceRecord.add(Pair.of(DwcTerm.maximumDepthInMeters,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS,
            "getDwcMaximumDepthInMeters")));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimDepth,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcVerbatimDepth")));
    occurrenceRecord.add(Pair.of(DwcTerm.maximumDistanceAboveSurfaceInMeters,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS,
            "getDwcMaximumDistanceAboveSurfaceInMeters")));
    occurrenceRecord.add(Pair.of(DwcTerm.minimumDistanceAboveSurfaceInMeters,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS,
            "getDwcMinimumDistanceAboveSurfaceInMeters")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.locationAccordingTo,
            retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS,
                "getDwcLocationAccordingTo")));
    occurrenceRecord.add(Pair.of(DwcTerm.locationRemarks,
        retrieveTerm(digitalSpecimen, LOCATION_FUNCTIONS, "getDwcLocationRemarks")));
  }

  private void mapGeologicalContext(
      DigitalSpecimen digitalSpecimen, ArrayList<Pair<Term, String>> occurrenceRecord)
      throws FailedProcessingException {
    occurrenceRecord.add(Pair.of(DwcTerm.geologicalContextID,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getId")));
    occurrenceRecord.add(Pair.of(DwcTerm.earliestEonOrLowestEonothem,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcEarliestEonOrLowestEonothem")));
    occurrenceRecord.add(Pair.of(DwcTerm.latestEonOrHighestEonothem,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcLatestEonOrHighestEonothem")));
    occurrenceRecord.add(Pair.of(DwcTerm.earliestEraOrLowestErathem,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcEarliestEraOrLowestErathem")));
    occurrenceRecord.add(Pair.of(DwcTerm.latestEraOrHighestErathem,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcLatestEraOrHighestErathem")));
    occurrenceRecord.add(Pair.of(DwcTerm.earliestPeriodOrLowestSystem,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcEarliestPeriodOrLowestSystem")));
    occurrenceRecord.add(Pair.of(DwcTerm.latestPeriodOrHighestSystem,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcLatestPeriodOrHighestSystem")));
    occurrenceRecord.add(Pair.of(DwcTerm.earliestEpochOrLowestSeries,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcEarliestEpochOrLowestSeries")));
    occurrenceRecord.add(Pair.of(DwcTerm.latestEpochOrHighestSeries,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcLatestEpochOrHighestSeries")));
    occurrenceRecord.add(Pair.of(DwcTerm.earliestAgeOrLowestStage,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcEarliestAgeOrLowestStage")));
    occurrenceRecord.add(Pair.of(DwcTerm.latestAgeOrHighestStage,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcLatestAgeOrHighestStage")));
    occurrenceRecord.add(Pair.of(DwcTerm.lowestBiostratigraphicZone,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcLowestBiostratigraphicZone")));
    occurrenceRecord.add(Pair.of(DwcTerm.highestBiostratigraphicZone,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcHighestBiostratigraphicZone")));
    occurrenceRecord.add(Pair.of(DwcTerm.lithostratigraphicTerms,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcLithostratigraphicTerms")));
    occurrenceRecord.add(Pair.of(DwcTerm.group,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcGroup")));
    occurrenceRecord.add(Pair.of(DwcTerm.formation,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcFormation")));
    occurrenceRecord.add(Pair.of(DwcTerm.member,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcMember")));
    occurrenceRecord.add(Pair.of(DwcTerm.bed,
        retrieveTerm(digitalSpecimen, GEOLOGICAL_CONTEXT_FUNCTIONS,
            "getDwcBed")));
  }

  private void addIdentifiers(DigitalSpecimen digitalSpecimen,
      ArrayList<List<Pair<Term, String>>> identificationRecords) {
    var identifiers = digitalSpecimen.getOdsHasIdentifiers().stream().filter(
            identifier -> !EXCLUDE_IDENTIFIERS.contains(identifier.getDctermsTitle()))
        .toList();
    for (var identifier : identifiers) {
      var identifierRecord = mapIdentifier(digitalSpecimen, identifier);
      identificationRecords.add(identifierRecord);
    }
  }

  private List<Pair<Term, String>> mapIdentifier(DigitalSpecimen digitalSpecimen,
      Identifier identifier) {
    var identifierRecord = new ArrayList<Pair<Term, String>>();
    identifierRecord.add(Pair.of(DwcaTerm.ID, digitalSpecimen.getId()));
    identifierRecord.add(Pair.of(DcTerm.identifier, identifier.getDctermsIdentifier()));
    identifierRecord.add(Pair.of(DcTerm.title, identifier.getDctermsTitle()));
    identifierRecord.add(
        Pair.of(DcTerm.subject, String.join(" | ", identifier.getDctermsSubject())));
    identifierRecord.add(
        Pair.of(DcTerm.format, String.join(" | ", identifier.getDctermsFormat())));
    identifierRecord.add(Pair.of(DwcTerm.datasetID, digitalSpecimen.getDwcDatasetID()));
    return identifierRecord;
  }

  private List<Pair<Term, String>> mapDigitalMedia(String digitalSpecimenId,
      DigitalMedia media) {
    var mediaRecord = new ArrayList<Pair<Term, String>>();
    mediaRecord.add(Pair.of(DwcaTerm.ID, digitalSpecimenId));
    mediaRecord.add(Pair.of(DcTerm.identifier, media.getDctermsIdentifier()));
    mediaRecord.add(Pair.of(DcTerm.type,
        media.getDctermsType() != null ? media.getDctermsType().value() : null));
    mediaRecord.add(Pair.of(DcTerm.format, media.getDctermsFormat()));
    mediaRecord.add(Pair.of(AcTerm.subtypeLiteral, media.getAcSubtypeLiteral()));
    mediaRecord.add(Pair.of(AcTerm.subtype, media.getAcSubtype()));
    mediaRecord.add(Pair.of(DcTerm.title, media.getDctermsTitle()));
    mediaRecord.add(Pair.of(DcTerm.modified, media.getDctermsModified()));
    mediaRecord.add(Pair.of(AcTerm.metadataLanguageLiteral, media.getAcMetadataLanguageLiteral()));
    mediaRecord.add(Pair.of(AcTerm.metadataLanguage, media.getAcMetadataLanguage()));
    mediaRecord.add(Pair.of(AcTerm.comments, media.getAcComments()));
    mediaRecord.add(Pair.of(DcTerm.available, media.getDctermsAvailable()));
    mediaRecord.add(Pair.of(DcTerm.rights, media.getDctermsRights()));
    mediaRecord.add(Pair.of(XmpRightsTerm.UsageTerms, media.getXmpRightsUsageTerms()));
    mediaRecord.add(Pair.of(XmpRightsTerm.WebStatement, media.getXmpRightsWebStatement()));
    mediaRecord.add(Pair.of(DcTerm.source, media.getDctermsSource()));
    mediaRecord.add(Pair.of(XmpTerm.CreateDate, media.getXmpCreateDate()));
    mediaRecord.add(Pair.of(DcTerm.description, media.getDctermsDescription()));
    mediaRecord.add(Pair.of(DcTerm.language, media.getDctermsLanguage()));
    mediaRecord.add(
        Pair.of(AcTerm.subjectCategoryVocabulary, media.getAcSubjectCategoryVocabulary()));
    mediaRecord.add(Pair.of(AcTerm.tag, String.join(", ", media.getAcTag())));
    mediaRecord.add(Pair.of(AcTerm.timeOfDay, media.getAcTimeOfDay()));
    mediaRecord.add(Pair.of(AcTerm.digitizationDate, media.getAcDigitizationDate()));
    mediaRecord.add(Pair.of(AcTerm.captureDevice, media.getAcCaptureDevice()));
    mediaRecord.add(
        Pair.of(AcTerm.resourceCreationTechnique, media.getAcResourceCreationTechnique()));
    mediaRecord.add(Pair.of(AcTerm.accessURI, media.getAcAccessURI()));
    mediaRecord.add(Pair.of(AcTerm.frameRate, getStringValue(media.getAcFrameRate())));
    mediaRecord.add(Pair.of(AcTerm.variantLiteral, media.getAcVariantLiteral()));
    mediaRecord.add(Pair.of(AcTerm.variant, media.getAcVariant()));
    mediaRecord.add(Pair.of(AcTerm.variantDescription, media.getAcVariantDescription()));
    mediaRecord.add(
        Pair.of(ExifTerm.PixelXDimension, getStringValue(media.getExifPixelXDimension())));
    mediaRecord.add(
        Pair.of(ExifTerm.PixelYDimension, getStringValue(media.getExifPixelYDimension())));
    return mediaRecord;
  }

  private void addOccurrence(DigitalSpecimen digitalSpecimen,
      List<List<Pair<Term, String>>> occurrenceList) throws FailedProcessingException {
    var occurrenceRecord = new ArrayList<Pair<Term, String>>();
    mapDigitalSpecimen(digitalSpecimen, occurrenceRecord);
    mapEvent(digitalSpecimen, occurrenceRecord);
    mapLocation(digitalSpecimen, occurrenceRecord);
    mapGeoreference(digitalSpecimen, occurrenceRecord);
    mapGeologicalContext(digitalSpecimen, occurrenceRecord);
    occurrenceList.add(occurrenceRecord);
  }

  private void addReference(List<Citation> citations, String digitalSpecimenId,
      List<List<Pair<Term, String>>> referenceRecords) {
    if (citations != null && !citations.isEmpty()) {
      for (var citation : citations) {
        var referenceRecord = new ArrayList<Pair<Term, String>>();
        referenceRecord.add(Pair.of(DwcaTerm.ID, digitalSpecimenId));
        referenceRecord.add(Pair.of(DcTerm.identifier, citation.getDctermsIdentifier()));
        referenceRecord.add(Pair.of(DcTerm.bibliographicCitation,
            citation.getDctermsBibliographicCitation()));
        referenceRecord.add(Pair.of(DcTerm.title, citation.getDctermsTitle()));
        referenceRecord.add(Pair.of(DcTerm.creator,
            retrieveCombinedAgentName(citation.getOdsHasAgents(), null)));
        referenceRecord.add(Pair.of(DcTerm.date, citation.getDctermsDate()));
        referenceRecord.add(Pair.of(DcTerm.description, citation.getDctermsDescription()));
        referenceRecord.add(Pair.of(DcTerm.type, citation.getDctermsType()));
        referenceRecords.add(referenceRecord);
      }
    }
  }

  private void mapDigitalSpecimen(DigitalSpecimen digitalSpecimen,
      List<Pair<Term, String>> occurrenceList) {
    occurrenceList.add(Pair.of(DwcaTerm.ID, digitalSpecimen.getId()));
    occurrenceList.add(Pair.of(DwcTerm.institutionID, digitalSpecimen.getOdsOrganisationID()));
    occurrenceList.add(Pair.of(DwcTerm.collectionID, digitalSpecimen.getDwcCollectionID()));
    occurrenceList.add(Pair.of(DwcTerm.datasetID, digitalSpecimen.getDwcDatasetID()));
    occurrenceList.add(Pair.of(DwcTerm.institutionCode, digitalSpecimen.getOdsOrganisationCode()));
    occurrenceList.add(Pair.of(DwcTerm.collectionCode, digitalSpecimen.getDwcCollectionCode()));
    occurrenceList.add(Pair.of(DwcTerm.datasetName, digitalSpecimen.getDwcDatasetName()));
    occurrenceList.add(Pair.of(DwcTerm.ownerInstitutionCode,
        digitalSpecimen.getOdsOwnerOrganisationCode()));
    occurrenceList.add(Pair.of(DwcTerm.basisOfRecord, digitalSpecimen.getDwcBasisOfRecord()));
    occurrenceList.add(Pair.of(DcTerm.license, digitalSpecimen.getDctermsLicense()));
    occurrenceList.add(Pair.of(DcTerm.type, "PhysicalObject"));
    occurrenceList.add(Pair.of(DcTerm.modified, digitalSpecimen.getDctermsModified()));
    occurrenceList.add(Pair.of(DcTerm.rightsHolder, digitalSpecimen.getDctermsRightsHolder()));
    occurrenceList.add(Pair.of(DcTerm.accessRights, digitalSpecimen.getDctermsAccessRights()));
    occurrenceList.add(Pair.of(DwcTerm.informationWithheld,
        digitalSpecimen.getDwcInformationWithheld()));
    occurrenceList.add(Pair.of(DwcTerm.dataGeneralizations,
        digitalSpecimen.getDwcDataGeneralizations()));
    occurrenceList.add(Pair.of(DwcTerm.occurrenceID,
        retrieveIdentifier(digitalSpecimen,
            List.of(OCCURRENCE_ID, UNIT_GUID, UNIT_ID))));
    occurrenceList.add(Pair.of(DwcTerm.catalogNumber,
        retrieveIdentifier(digitalSpecimen, List.of("dwc:catalogNumber", UNIT_ID))));
    occurrenceList.add(Pair.of(DwcTerm.recordedBy,
        retrieveCombinedAgentName(digitalSpecimen.getOdsHasAgents(), "collector")));
    occurrenceList.add(Pair.of(DwcTerm.recordNumber,
        retrieveIdentifier(digitalSpecimen, List.of("dwc:recordNumber", "abcd:recordURI"))));
    occurrenceList.add(Pair.of(DwcTerm.recordedByID,
        retrieveCombinedAgentId(digitalSpecimen.getOdsHasAgents(), "collector")));
    occurrenceList.add(Pair.of(DwcTerm.organismQuantity, digitalSpecimen.getDwcOrganismQuantity()));
    occurrenceList.add(Pair.of(DwcTerm.organismQuantityType,
        digitalSpecimen.getDwcOrganismQuantityType()));
    occurrenceList.add(Pair.of(DwcTerm.preparations, digitalSpecimen.getDwcPreparations()));
    occurrenceList.add(Pair.of(DwcTerm.disposition, digitalSpecimen.getDwcDisposition()));
    occurrenceList.add(Pair.of(DwcTerm.organismID, digitalSpecimen.getDwcOrganismID()));
    occurrenceList.add(Pair.of(DwcTerm.organismName, digitalSpecimen.getDwcOrganismName()));
    occurrenceList.add(Pair.of(DwcTerm.organismScope, digitalSpecimen.getDwcOrganismScope()));
    occurrenceList.add(Pair.of(DwcTerm.organismRemarks, digitalSpecimen.getDwcOrganismRemarks()));
    occurrenceList.add(
        Pair.of(DwcTerm.materialEntityID, digitalSpecimen.getOdsPhysicalSpecimenID()));
    occurrenceList.add(Pair.of(DwcTerm.verbatimLabel, digitalSpecimen.getDwcVerbatimLabel()));
  }

  private void mapGeoreference(DigitalSpecimen digitalSpecimen,
      ArrayList<Pair<Term, String>> occurrenceRecord) throws FailedProcessingException {
    occurrenceRecord.add(
        Pair.of(DwcTerm.decimalLatitude,
            retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
                "getDwcDecimalLatitude")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.decimalLongitude,
            retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
                "getDwcDecimalLongitude")));
    occurrenceRecord.add(Pair.of(DwcTerm.geodeticDatum,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
            "getDwcGeodeticDatum")));
    occurrenceRecord.add(Pair.of(DwcTerm.coordinateUncertaintyInMeters,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
            "getDwcCoordinateUncertaintyInMeters")));
    occurrenceRecord.add(Pair.of(DwcTerm.coordinatePrecision,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
            "getDwcCoordinatePrecision")));
    occurrenceRecord.add(Pair.of(DwcTerm.pointRadiusSpatialFit,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
            "getDwcPointRadiusSpatialFit")));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimCoordinates,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
            "getDwcVerbatimCoordinates")));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimLatitude,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
            "getDwcVerbatimLatitude")));
    occurrenceRecord.add(
        Pair.of(DwcTerm.verbatimLongitude,
            retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
                "getDwcVerbatimLongitude")));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimCoordinateSystem,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
            "getDwcVerbatimCoordinateSystem")));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimSRS,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS, "getDwcVerbatimSRS")));
    occurrenceRecord.add(Pair.of(DwcTerm.footprintWKT,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS, "getDwcFootprintWKT")));
    occurrenceRecord.add(Pair.of(DwcTerm.footprintSRS,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS, "getDwcFootprintSRS")));
    occurrenceRecord.add(Pair.of(DwcTerm.footprintSpatialFit,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
            "getDwcFootprintSpatialFit")));
    occurrenceRecord.add(Pair.of(DwcTerm.georeferencedBy,
        retrieveCombinedAgentName(retrieveGeoreferenceAgent(digitalSpecimen), null)));
    occurrenceRecord.add(Pair.of(DwcTerm.georeferencedDate,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
            "getDwcGeoreferencedDate")));
    occurrenceRecord.add(Pair.of(DwcTerm.georeferenceProtocol,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
            "getDwcGeoreferenceProtocol")));
    occurrenceRecord.add(Pair.of(DwcTerm.georeferenceSources,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
            "getDwcGeoreferenceSources")));
    occurrenceRecord.add(Pair.of(DwcTerm.georeferenceRemarks,
        retrieveTerm(digitalSpecimen, GEOREFERENCE_FUNCTIONS,
            "getDwcGeoreferenceRemarks")));
  }

  private List<Agent> retrieveGeoreferenceAgent(DigitalSpecimen digitalSpecimen) {
    if (GEOREFERENCE_FUNCTIONS.getLeft().test(digitalSpecimen)) {
      return null;
    }
    return digitalSpecimen.getOdsHasEvents().getFirst().getOdsHasLocation().getOdsHasGeoreference()
        .getOdsHasAgents();
  }

  private void addIdentifications(DigitalSpecimen digitalSpecimen,
      List<List<Pair<Term, String>>> identificationRecords,
      List<List<Pair<Term, String>>> referenceRecords) {
    for (var identification : digitalSpecimen.getOdsHasIdentifications()) {
      for (var taxonIdentification : identification.getOdsHasTaxonIdentifications()) {
        identificationRecords.add(
            mapIdentification(digitalSpecimen, identification, taxonIdentification));
        addReference(identification.getOdsHasCitations(), digitalSpecimen.getId(),
            referenceRecords);
      }
    }
  }

  private List<Pair<Term, String>> mapIdentification(DigitalSpecimen digitalSpecimen,
      Identification identification, TaxonIdentification taxonIdentification) {
    var identificationRecord = new ArrayList<Pair<Term, String>>();
    identificationRecord.add(Pair.of(DwcaTerm.ID, digitalSpecimen.getId()));
    identificationRecord.add(Pair.of(DwcTerm.identificationID, identification.getId()));
    identificationRecord.add(Pair.of(DwcTerm.verbatimIdentification,
        identification.getDwcVerbatimIdentification()));
    identificationRecord.add(Pair.of(DwcTerm.identificationQualifier,
        identification.getDwcIdentificationQualifier()));
    identificationRecord.add(Pair.of(DwcTerm.typeStatus, identification.getDwcTypeStatus()));
    identificationRecord.add(Pair.of(DwcTerm.identifiedBy,
        retrieveCombinedAgentName(identification.getOdsHasAgents(), "identifier")));
    identificationRecord.add(Pair.of(DwcTerm.identifiedByID,
        retrieveCombinedAgentId(identification.getOdsHasAgents(), "identifier")));
    identificationRecord.add(Pair.of(DwcTerm.dateIdentified,
        identification.getDwcDateIdentified()));
    identificationRecord.add(Pair.of(DwcTerm.identificationVerificationStatus,
        getStringValue(identification.getOdsIsVerifiedIdentification())));
    identificationRecord.add(Pair.of(DwcTerm.identificationRemarks,
        identification.getDwcIdentificationRemarks()));
    identificationRecord.add(Pair.of(DwcTerm.taxonID, taxonIdentification.getDwcTaxonID()));
    identificationRecord.add(Pair.of(DwcTerm.scientificNameID,
        taxonIdentification.getDwcScientificNameID()));
    identificationRecord.add(Pair.of(DwcTerm.acceptedNameUsageID,
        taxonIdentification.getDwcAcceptedNameUsageID()));
    identificationRecord.add(Pair.of(DwcTerm.scientificName,
        taxonIdentification.getDwcScientificName()));
    identificationRecord.add(Pair.of(DwcTerm.acceptedNameUsage,
        taxonIdentification.getDwcAcceptedNameUsage()));
    identificationRecord.add(Pair.of(DwcTerm.originalNameUsage,
        taxonIdentification.getDwcOriginalNameUsage()));
    identificationRecord.add(Pair.of(DwcTerm.namePublishedInYear,
        taxonIdentification.getDwcNamePublishedInYear()));
    identificationRecord.add(Pair.of(DwcTerm.kingdom, taxonIdentification.getDwcKingdom()));
    identificationRecord.add(Pair.of(DwcTerm.phylum, taxonIdentification.getDwcPhylum()));
    identificationRecord.add(Pair.of(DwcTerm.class_, taxonIdentification.getDwcClass()));
    identificationRecord.add(Pair.of(DwcTerm.order, taxonIdentification.getDwcOrder()));
    identificationRecord.add(Pair.of(DwcTerm.superfamily, taxonIdentification.getDwcSuperfamily()));
    identificationRecord.add(Pair.of(DwcTerm.family, taxonIdentification.getDwcFamily()));
    identificationRecord.add(Pair.of(DwcTerm.subfamily, taxonIdentification.getDwcSubfamily()));
    identificationRecord.add(Pair.of(DwcTerm.tribe, taxonIdentification.getDwcTribe()));
    identificationRecord.add(Pair.of(DwcTerm.subtribe, taxonIdentification.getDwcSubtribe()));
    identificationRecord.add(Pair.of(DwcTerm.genus, taxonIdentification.getDwcGenus()));
    identificationRecord.add(Pair.of(DwcTerm.genericName, taxonIdentification.getDwcGenericName()));
    identificationRecord.add(Pair.of(DwcTerm.subgenus, taxonIdentification.getDwcSubgenus()));
    identificationRecord.add(Pair.of(DwcTerm.infragenericEpithet,
        taxonIdentification.getDwcInfragenericEpithet()));
    identificationRecord.add(Pair.of(DwcTerm.specificEpithet,
        taxonIdentification.getDwcSpecificEpithet()));
    identificationRecord.add(Pair.of(DwcTerm.infraspecificEpithet,
        taxonIdentification.getDwcInfraspecificEpithet()));
    identificationRecord.add(Pair.of(DwcTerm.cultivarEpithet,
        taxonIdentification.getDwcCultivarEpithet()));
    identificationRecord.add(Pair.of(DwcTerm.taxonRank, taxonIdentification.getDwcTaxonRank()));
    identificationRecord.add(Pair.of(DwcTerm.verbatimTaxonRank,
        taxonIdentification.getDwcVerbatimTaxonRank()));
    identificationRecord.add(Pair.of(DwcTerm.scientificNameAuthorship,
        taxonIdentification.getDwcScientificNameAuthorship()));
    identificationRecord.add(Pair.of(DwcTerm.vernacularName,
        taxonIdentification.getDwcVernacularName()));
    identificationRecord.add(Pair.of(DwcTerm.nomenclaturalCode,
        taxonIdentification.getDwcNomenclaturalCode()));
    identificationRecord.add(Pair.of(DwcTerm.taxonomicStatus,
        taxonIdentification.getDwcTaxonomicStatus()));
    identificationRecord.add(Pair.of(DwcTerm.nomenclaturalStatus,
        taxonIdentification.getDwcNomenclaturalStatus()));
    identificationRecord.add(Pair.of(DwcTerm.taxonRemarks,
        taxonIdentification.getDwcTaxonRemarks()));
    return identificationRecord;
  }

  private void addRelationships(DigitalSpecimen digitalSpecimen,
      ArrayList<List<Pair<Term, String>>> relationshipList) {
    var relationships = digitalSpecimen.getOdsHasEntityRelationships().stream()
        .filter(er -> !EXCLUDE_RELATIONSHIPS.contains(er.getDwcRelationshipOfResource()))
        .toList();
    for (var relationship : relationships) {
      relationshipList.add(mapRelationship(digitalSpecimen, relationship));
    }
  }

  private List<Pair<Term, String>> mapRelationship(DigitalSpecimen digitalSpecimen,
      EntityRelationship relationship) {
    var relationshipRecord = new ArrayList<Pair<Term, String>>();
    relationshipRecord.add(Pair.of(DwcaTerm.ID, digitalSpecimen.getId()));
    relationshipRecord.add(Pair.of(DwcTerm.resourceRelationshipID, relationship.getId()));
    relationshipRecord.add(Pair.of(DwcTerm.resourceID, digitalSpecimen.getId()));
    relationshipRecord.add(Pair.of(DwcTerm.relationshipOfResourceID,
        relationship.getDwcRelationshipOfResourceID()));
    relationshipRecord.add(Pair.of(DwcTerm.relatedResourceID,
        relationship.getDwcRelatedResourceID()));
    relationshipRecord.add(Pair.of(DwcTerm.relationshipOfResource,
        relationship.getDwcRelationshipOfResource()));
    relationshipRecord.add(Pair.of(DwcTerm.relationshipAccordingTo,
        retrieveCombinedAgentName(relationship.getOdsHasAgents(), null)));
    relationshipRecord.add(Pair.of(DwcTerm.relationshipEstablishedDate,
        relationship.getDwcRelationshipEstablishedDate() != null
            ? relationship.getDwcRelationshipEstablishedDate().toString() : null));
    relationshipRecord.add(Pair.of(DwcTerm.relationshipRemarks,
        relationship.getDwcRelationshipRemarks()));
    return relationshipRecord;
  }

  // Collect any assertion on either specimen or associated first event (as only first event is included in the occurrence record)
  private void addAnnotation(DigitalSpecimen digitalSpecimen,
      ArrayList<List<Pair<Term, String>>> assertionList) {
    if (digitalSpecimen.getOdsHasAssertions() != null && !digitalSpecimen.getOdsHasAssertions()
        .isEmpty()) {
      for (var assertion : digitalSpecimen.getOdsHasAssertions()) {
        mapAssertion(digitalSpecimen, assertionList, assertion);
      }
    }
    if (digitalSpecimen.getOdsHasEvents() != null && !digitalSpecimen.getOdsHasEvents().isEmpty()) {
      var event = digitalSpecimen.getOdsHasEvents().getFirst();
      if (event.getOdsHasAssertions() != null && !event.getOdsHasAssertions().isEmpty()) {
        for (var assertion : event.getOdsHasAssertions()) {
          mapAssertion(digitalSpecimen, assertionList, assertion);
        }
      }
    }
  }

  private void mapAssertion(DigitalSpecimen digitalSpecimen,
      ArrayList<List<Pair<Term, String>>> assertionList, Assertion assertion) {
    var assertionRecord = new ArrayList<Pair<Term, String>>();
    assertionRecord.add(Pair.of(DwcaTerm.ID, digitalSpecimen.getId()));
    assertionRecord.add(Pair.of(DwcTerm.measurementID, assertion.getDwcMeasurementID()));
    assertionRecord.add(Pair.of(DwcTerm.occurrenceID,
        retrieveIdentifier(digitalSpecimen, List.of(OCCURRENCE_ID, UNIT_GUID, UNIT_ID))));
    assertionRecord.add(Pair.of(DwcTerm.measurementType, assertion.getDwcMeasurementType()));
    assertionRecord.add(Pair.of(ObisTerm.measurementTypeID, assertion.getDwciriMeasurementType()));
    assertionRecord.add(Pair.of(DwcTerm.measurementValue, assertion.getDwcMeasurementValue()));
    assertionRecord.add(
        Pair.of(ObisTerm.measurementValueID, assertion.getDwciriMeasurementValue()));
    assertionRecord.add(
        Pair.of(DwcTerm.measurementAccuracy, assertion.getDwcMeasurementAccuracy()));
    assertionRecord.add(Pair.of(DwcTerm.measurementUnit, assertion.getDwcMeasurementUnit()));
    assertionRecord.add(Pair.of(ObisTerm.measurementUnitID, assertion.getDwciriMeasurementUnit()));
    assertionRecord.add(
        Pair.of(DwcTerm.measurementDeterminedDate, assertion.getDwcMeasurementDeterminedDate()));
    assertionRecord.add(Pair.of(DwcTerm.measurementDeterminedBy,
        retrieveCombinedAgentName(assertion.getOdsHasAgents(), "measurer")));
    assertionRecord.add(Pair.of(DwcTerm.measurementRemarks, assertion.getDwcMeasurementRemarks()));
    assertionList.add(assertionRecord);
  }

  private Map<String, List<DigitalMedia>> createSpecimenToMediaMapping(
      List<DigitalSpecimen> digitalSpecimenList, List<DigitalMedia> digitalMediaList) {
    var digitalSpecimenIds = digitalSpecimenList.stream().map(DigitalSpecimen::getId).collect(
        Collectors.toSet());
    var specimenMediaMap = digitalMediaList.stream()
        .collect(Collectors.groupingBy(media -> media.getOdsHasEntityRelationships().stream()
            .filter(er -> er.getDwcRelationshipOfResource().equals("hasDigitalSpecimen"))
            .map(er -> er.getOdsRelatedResourceURI().toString()).findFirst().orElseThrow()));
    // We need to remove any media that does not have a corresponding digital specimen
    // This is possible because a media item can be linked to multiple digital specimens
    specimenMediaMap.keySet().removeIf(id -> !digitalSpecimenIds.contains(id));
    return specimenMediaMap;
  }

  private void addDigitalMedia(List<DigitalMedia> digitalMedia, String digitalSpecimenId,
      ArrayList<List<Pair<Term, String>>> digitalMediaList) {
    for (var media : digitalMedia) {
      digitalMediaList.add(mapDigitalMedia(digitalSpecimenId, media));
    }
  }

  private List<String> getMediaIds(List<DigitalSpecimen> digitalSpecimenList) {
    return digitalSpecimenList.stream()
        .map(DigitalSpecimen::getOdsHasEntityRelationships)
        .flatMap(List::stream)
        .filter(er -> er.getDwcRelationshipOfResource().equals("hasDigitalMedia"))
        .map(er -> er.getOdsRelatedResourceURI().toString()).toList();
  }


  @Override
  protected List<String> targetFields() {
    return List.of();
  }
}
