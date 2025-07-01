package eu.dissco.exportjob.service;

import static eu.dissco.exportjob.utils.ExportUtils.EXCLUDE_IDENTIFIERS;
import static eu.dissco.exportjob.utils.ExportUtils.EXCLUDE_RELATIONSHIPS;
import static eu.dissco.exportjob.utils.ExportUtils.retrieveAgentIds;
import static eu.dissco.exportjob.utils.ExportUtils.retrieveAgentNames;

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
import eu.dissco.exportjob.schema.Citation;
import eu.dissco.exportjob.schema.DigitalMedia;
import eu.dissco.exportjob.schema.DigitalSpecimen;
import eu.dissco.exportjob.schema.EntityRelationship;
import eu.dissco.exportjob.schema.Event;
import eu.dissco.exportjob.schema.GeologicalContext;
import eu.dissco.exportjob.schema.Georeference;
import eu.dissco.exportjob.schema.Identification;
import eu.dissco.exportjob.schema.Identifier;
import eu.dissco.exportjob.schema.Location;
import eu.dissco.exportjob.schema.TaxonIdentification;
import eu.dissco.exportjob.utils.ExportUtils;
import eu.dissco.exportjob.web.ExporterBackendClient;
import freemarker.template.TemplateException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.gbif.dwc.terms.AcTerm;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.DwcaTerm;
import org.gbif.dwc.terms.ExifTerm;
import org.gbif.dwc.terms.GbifTerm;
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

  private static void mapEvent(Event event, ArrayList<Pair<Term, String>> occurrenceRecord) {
    occurrenceRecord.add(Pair.of(DwcTerm.sex, event.getDwcSex()));
    occurrenceRecord.add(Pair.of(DwcTerm.lifeStage, event.getDwcLifeStage()));
    occurrenceRecord.add(
        Pair.of(DwcTerm.reproductiveCondition, event.getDwcReproductiveCondition()));
    occurrenceRecord.add(Pair.of(DwcTerm.caste, event.getDwcCaste()));
    occurrenceRecord.add(Pair.of(DwcTerm.behavior, event.getDwcBehavior()));
    occurrenceRecord.add(Pair.of(DwcTerm.vitality, event.getDwcVitality()));
    occurrenceRecord.add(Pair.of(DwcTerm.establishmentMeans, event.getDwcEstablishmentMeans()));
    occurrenceRecord.add(
        Pair.of(DwcTerm.degreeOfEstablishment, event.getDwcDegreeOfEstablishment()));
    occurrenceRecord.add(Pair.of(DwcTerm.pathway, event.getDwcPathway()));
    occurrenceRecord.add(Pair.of(DwcTerm.georeferenceVerificationStatus,
        event.getDwcGeoreferenceVerificationStatus()));
    occurrenceRecord.add(Pair.of(DwcTerm.eventType, event.getDwcEventType()));
    occurrenceRecord.add(Pair.of(DwcTerm.fieldNotes, event.getDwcFieldNumber()));
    occurrenceRecord.add(Pair.of(DwcTerm.eventDate, event.getDwcEventDate()));
    occurrenceRecord.add(Pair.of(DwcTerm.eventTime, event.getDwcEventTime()));
    occurrenceRecord.add(
        Pair.of(DwcTerm.startDayOfYear, String.valueOf(event.getDwcStartDayOfYear())));
    occurrenceRecord.add(Pair.of(DwcTerm.endDayOfYear, String.valueOf(event.getDwcEndDayOfYear())));
    occurrenceRecord.add(Pair.of(DwcTerm.year, String.valueOf(event.getDwcYear())));
    occurrenceRecord.add(Pair.of(DwcTerm.month, String.valueOf(event.getDwcMonth())));
    occurrenceRecord.add(Pair.of(DwcTerm.day, String.valueOf(event.getDwcDay())));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimEventDate, event.getDwcVerbatimEventDate()));
    occurrenceRecord.add(Pair.of(DwcTerm.habitat, event.getDwcHabitat()));
    occurrenceRecord.add(Pair.of(DwcTerm.samplingProtocol, event.getDwcSamplingProtocol()));
    occurrenceRecord.add(
        Pair.of(DwcTerm.sampleSizeValue, String.valueOf(event.getDwcSampleSizeValue())));
    occurrenceRecord.add(Pair.of(DwcTerm.sampleSizeUnit, event.getDwcSampleSizeUnit()));
    occurrenceRecord.add(Pair.of(DwcTerm.samplingEffort, event.getDwcSamplingEffort()));
    occurrenceRecord.add(Pair.of(DwcTerm.fieldNotes, event.getDwcFieldNotes()));
    occurrenceRecord.add(Pair.of(DwcTerm.eventRemarks, event.getDwcEventRemarks()));
    occurrenceRecord.add(Pair.of(DwcTerm.eventID, event.getId()));
  }

  private static void mapLocation(Location location,
      ArrayList<Pair<Term, String>> occurrenceRecord) {
    occurrenceRecord.add(Pair.of(DwcTerm.locationID, location.getId()));
    occurrenceRecord.add(Pair.of(DwcTerm.higherGeography, location.getDwcHigherGeography()));
    occurrenceRecord.add(Pair.of(DwcTerm.higherGeographyID, location.getDwcHigherGeographyID()));
    occurrenceRecord.add(Pair.of(DwcTerm.continent, location.getDwcContinent()));
    occurrenceRecord.add(Pair.of(DwcTerm.waterBody, location.getDwcWaterBody()));
    occurrenceRecord.add(Pair.of(DwcTerm.island, location.getDwcIsland()));
    occurrenceRecord.add(Pair.of(DwcTerm.islandGroup, location.getDwcIslandGroup()));
    occurrenceRecord.add(Pair.of(DwcTerm.country, location.getDwcCountry()));
    occurrenceRecord.add(Pair.of(DwcTerm.countryCode, location.getDwcCountryCode()));
    occurrenceRecord.add(Pair.of(DwcTerm.stateProvince, location.getDwcStateProvince()));
    occurrenceRecord.add(Pair.of(DwcTerm.county, location.getDwcCounty()));
    occurrenceRecord.add(Pair.of(DwcTerm.municipality, location.getDwcMunicipality()));
    occurrenceRecord.add(Pair.of(DwcTerm.locality, location.getDwcLocality()));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimLocality, location.getDwcVerbatimLocality()));
    occurrenceRecord.add(Pair.of(DwcTerm.minimumElevationInMeters,
        String.valueOf(location.getDwcMinimumElevationInMeters())));
    occurrenceRecord.add(Pair.of(DwcTerm.maximumElevationInMeters,
        String.valueOf(location.getDwcMaximumElevationInMeters())));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimElevation, location.getDwcVerbatimElevation()));
    occurrenceRecord.add(Pair.of(DwcTerm.minimumDepthInMeters,
        String.valueOf(location.getDwcMinimumDepthInMeters())));
    occurrenceRecord.add(Pair.of(DwcTerm.maximumDepthInMeters,
        String.valueOf(location.getDwcMaximumDepthInMeters())));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimDepth, location.getDwcVerbatimDepth()));
    occurrenceRecord.add(Pair.of(DwcTerm.maximumDistanceAboveSurfaceInMeters,
        String.valueOf(location.getDwcMaximumDistanceAboveSurfaceInMeters())));
    occurrenceRecord.add(Pair.of(DwcTerm.minimumDistanceAboveSurfaceInMeters,
        String.valueOf(location.getDwcMinimumDistanceAboveSurfaceInMeters())));
    occurrenceRecord.add(
        Pair.of(DwcTerm.locationAccordingTo, location.getDwcLocationAccordingTo()));
    occurrenceRecord.add(Pair.of(DwcTerm.locationRemarks, location.getDwcLocationRemarks()));
  }

  private static void mapGeologicalContext(
      GeologicalContext geologicalContext, ArrayList<Pair<Term, String>> occurrenceRecord) {
    occurrenceRecord.add(Pair.of(DwcTerm.geologicalContextID, geologicalContext.getId()));
    occurrenceRecord.add(Pair.of(DwcTerm.earliestEonOrLowestEonothem,
        geologicalContext.getDwcEarliestEonOrLowestEonothem()));
    occurrenceRecord.add(Pair.of(DwcTerm.latestEonOrHighestEonothem,
        geologicalContext.getDwcLatestEonOrHighestEonothem()));
    occurrenceRecord.add(Pair.of(DwcTerm.earliestEraOrLowestErathem,
        geologicalContext.getDwcEarliestEraOrLowestErathem()));
    occurrenceRecord.add(Pair.of(DwcTerm.latestEraOrHighestErathem,
        geologicalContext.getDwcLatestEraOrHighestErathem()));
    occurrenceRecord.add(Pair.of(DwcTerm.earliestPeriodOrLowestSystem,
        geologicalContext.getDwcEarliestPeriodOrLowestSystem()));
    occurrenceRecord.add(Pair.of(DwcTerm.latestPeriodOrHighestSystem,
        geologicalContext.getDwcLatestPeriodOrHighestSystem()));
    occurrenceRecord.add(Pair.of(DwcTerm.earliestEpochOrLowestSeries,
        geologicalContext.getDwcEarliestEpochOrLowestSeries()));
    occurrenceRecord.add(Pair.of(DwcTerm.latestEpochOrHighestSeries,
        geologicalContext.getDwcLatestEpochOrHighestSeries()));
    occurrenceRecord.add(Pair.of(DwcTerm.earliestAgeOrLowestStage,
        geologicalContext.getDwcEarliestAgeOrLowestStage()));
    occurrenceRecord.add(Pair.of(DwcTerm.latestAgeOrHighestStage,
        geologicalContext.getDwcLatestAgeOrHighestStage()));
    occurrenceRecord.add(Pair.of(DwcTerm.lowestBiostratigraphicZone,
        geologicalContext.getDwcLowestBiostratigraphicZone()));
    occurrenceRecord.add(Pair.of(DwcTerm.highestBiostratigraphicZone,
        geologicalContext.getDwcHighestBiostratigraphicZone()));
    occurrenceRecord.add(Pair.of(DwcTerm.lithostratigraphicTerms,
        geologicalContext.getDwcLithostratigraphicTerms()));
    occurrenceRecord.add(Pair.of(DwcTerm.group, geologicalContext.getDwcGroup()));
    occurrenceRecord.add(Pair.of(DwcTerm.formation, geologicalContext.getDwcFormation()));
    occurrenceRecord.add(Pair.of(DwcTerm.member, geologicalContext.getDwcMember()));
    occurrenceRecord.add(Pair.of(DwcTerm.bed, geologicalContext.getDwcBed()));
  }

  private static void addIdentifiers(DigitalSpecimen digitalSpecimen,
      ArrayList<List<Pair<Term, String>>> identificationRecords) {
    var identifiers = digitalSpecimen.getOdsHasIdentifiers().stream().filter(
            identifier -> !EXCLUDE_IDENTIFIERS.contains(identifier.getDctermsTitle()))
        .toList();
    for (var identifier : identifiers) {
      var identifierRecord = mapIdentifier(digitalSpecimen, identifier);
      identificationRecords.add(identifierRecord);
    }
  }

  private static List<Pair<Term, String>> mapIdentifier(DigitalSpecimen digitalSpecimen,
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

  private static List<Pair<Term, String>> mapDigitalMedia(String digitalSpecimenId,
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
    mediaRecord.add(Pair.of(AcTerm.frameRate, String.valueOf(media.getAcFrameRate())));
    mediaRecord.add(Pair.of(AcTerm.variantLiteral, media.getAcVariantLiteral()));
    mediaRecord.add(Pair.of(AcTerm.variant, media.getAcVariant()));
    mediaRecord.add(Pair.of(AcTerm.variantDescription, media.getAcVariantDescription()));
    mediaRecord.add(
        Pair.of(ExifTerm.PixelXDimension, String.valueOf(media.getExifPixelXDimension())));
    mediaRecord.add(
        Pair.of(ExifTerm.PixelYDimension, String.valueOf(media.getExifPixelYDimension())));
    return mediaRecord;
  }

  private static void addGeologicalContext(Location location,
      ArrayList<Pair<Term, String>> occurrenceRecord) {
    if (location.getOdsHasGeologicalContext() != null) {
      var geologicalContext = location.getOdsHasGeologicalContext();
      mapGeologicalContext(geologicalContext, occurrenceRecord);
    }
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
  protected void processSearchResults(List<JsonNode> searchResults) throws IOException {
    var digitalSpecimenList = searchResults.stream()
        .map(json -> objectMapper.convertValue(json, DigitalSpecimen.class)).toList();
    var digitalMediaList = elasticSearchRepository.getTargetMediaById(
            getMediaIds(digitalSpecimenList)).stream()
        .map(json -> objectMapper.convertValue(json, DigitalMedia.class)).toList();
    var specimenToDigitalMediaMapping = createSpecimenToMediaMapping(digitalSpecimenList,
        digitalMediaList);
    var mappedResult = mapToDwcaRecords(digitalSpecimenList, specimenToDigitalMediaMapping);
    dwcaZipWriter.write(mappedResult);
  }

  // We need to remove any media that does not have a corresponding digital specimen
  // This is possible because a media item can be linked to multiple digital specimens
  private Map<String, List<DigitalMedia>> createSpecimenToMediaMapping(
      List<DigitalSpecimen> digitalSpecimenList, List<DigitalMedia> digitalMediaList) {
    var digitalSpecimenIds = digitalSpecimenList.stream().map(DigitalSpecimen::getId).toList();
    var specimenMediaMap = digitalMediaList.stream()
        .collect(Collectors.groupingBy(media -> media.getOdsHasEntityRelationships().stream()
            .filter(er -> er.getDwcRelationshipOfResource().equals("hasDigitalSpecimen"))
            .map(er -> er.getOdsRelatedResourceURI().toString()).findFirst().get()));
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

  private Map<Term, List<List<Pair<Term, String>>>> mapToDwcaRecords(
      List<DigitalSpecimen> digitalSpecimenList,
      Map<String, List<DigitalMedia>> specimenToDigitalMediaMapping) {
    var mappedList = new HashMap<Term, List<List<Pair<Term, String>>>>();
    var occurrenceList = new ArrayList<List<Pair<Term, String>>>();
    var identificationList = new ArrayList<List<Pair<Term, String>>>();
    var referenceList = new ArrayList<List<Pair<Term, String>>>();
    var identifierList = new ArrayList<List<Pair<Term, String>>>();
    var relationshipList = new ArrayList<List<Pair<Term, String>>>();
    var digitalMediaList = new ArrayList<List<Pair<Term, String>>>();
    for (var digitalSpecimen : digitalSpecimenList) {
      occurrenceList.add(mapToOccurrence(digitalSpecimen));
      addIdentifications(digitalSpecimen, identificationList, referenceList);
      addIdentifiers(digitalSpecimen, identifierList);
      addRelationships(digitalSpecimen, relationshipList);
      addReference(digitalSpecimen.getOdsHasCitations(), digitalSpecimen.getId(), referenceList);
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
    return mappedList;
  }

  private List<Pair<Term, String>> mapToOccurrence(DigitalSpecimen digitalSpecimen) {
    var occurrenceRecord = new ArrayList<Pair<Term, String>>();
    mapDigitalSpecimen(digitalSpecimen, occurrenceRecord);
    if (digitalSpecimen.getOdsHasEvents() != null && !digitalSpecimen.getOdsHasEvents()
        .isEmpty()) {
      var event = digitalSpecimen.getOdsHasEvents().getFirst();
      mapEvent(event, occurrenceRecord);
      if (event.getOdsHasLocation() != null) {
        var location = event.getOdsHasLocation();
        mapLocation(location, occurrenceRecord);
        addGeoreference(location, occurrenceRecord);
        addGeologicalContext(location, occurrenceRecord);
      }
    }
    return occurrenceRecord;
  }

  private void addGeoreference(Location location, ArrayList<Pair<Term, String>> occurrenceRecord) {
    if (location.getOdsHasGeoreference() != null) {
      var georeference = location.getOdsHasGeoreference();
      mapGeoreference(georeference, occurrenceRecord);
    }
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
            retrieveAgentNames(citation.getOdsHasAgents(), null)));
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
    occurrenceList.add(Pair.of(DwcTerm.occurrenceID, digitalSpecimen.getId()));
    occurrenceList.add(Pair.of(DwcTerm.catalogNumber,
        ExportUtils.retrieveSpecificIdentifier(digitalSpecimen, "dwc:catalogNumber")));
    occurrenceList.add(Pair.of(DwcTerm.recordedBy,
        retrieveAgentNames(digitalSpecimen.getOdsHasAgents(), "collector")));
    occurrenceList.add(Pair.of(DwcTerm.recordedByID,
        retrieveAgentIds(digitalSpecimen.getOdsHasAgents(), "collector")));
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

  private void mapGeoreference(Georeference georeference,
      ArrayList<Pair<Term, String>> occurrenceRecord) {
    occurrenceRecord.add(
        Pair.of(DwcTerm.decimalLatitude, String.valueOf(georeference.getDwcDecimalLatitude())));
    occurrenceRecord.add(
        Pair.of(DwcTerm.decimalLongitude, String.valueOf(georeference.getDwcDecimalLongitude())));
    occurrenceRecord.add(Pair.of(DwcTerm.geodeticDatum, georeference.getDwcGeodeticDatum()));
    occurrenceRecord.add(Pair.of(DwcTerm.coordinateUncertaintyInMeters,
        String.valueOf(georeference.getDwcCoordinateUncertaintyInMeters())));
    occurrenceRecord.add(Pair.of(DwcTerm.coordinatePrecision,
        String.valueOf(georeference.getDwcCoordinatePrecision())));
    occurrenceRecord.add(Pair.of(DwcTerm.pointRadiusSpatialFit,
        String.valueOf(georeference.getDwcPointRadiusSpatialFit())));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimCoordinates,
        georeference.getDwcVerbatimCoordinates()));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimLatitude, georeference.getDwcVerbatimLatitude()));
    occurrenceRecord.add(
        Pair.of(DwcTerm.verbatimLongitude, georeference.getDwcVerbatimLongitude()));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimCoordinateSystem,
        georeference.getDwcVerbatimCoordinateSystem()));
    occurrenceRecord.add(Pair.of(DwcTerm.verbatimSRS, georeference.getDwcVerbatimSRS()));
    occurrenceRecord.add(Pair.of(DwcTerm.footprintWKT, georeference.getDwcFootprintWKT()));
    occurrenceRecord.add(Pair.of(DwcTerm.footprintSRS, georeference.getDwcFootprintSRS()));
    occurrenceRecord.add(Pair.of(DwcTerm.footprintSpatialFit,
        String.valueOf(georeference.getDwcFootprintSpatialFit())));
    occurrenceRecord.add(Pair.of(DwcTerm.georeferencedBy,
        retrieveAgentNames(georeference.getOdsHasAgents(), null)));
    occurrenceRecord.add(
        Pair.of(DwcTerm.georeferencedDate, georeference.getDwcGeoreferencedDate()));
    occurrenceRecord.add(Pair.of(DwcTerm.georeferenceProtocol,
        georeference.getDwcGeoreferenceProtocol()));
    occurrenceRecord.add(Pair.of(DwcTerm.georeferenceSources,
        georeference.getDwcGeoreferenceSources()));
    occurrenceRecord.add(Pair.of(DwcTerm.georeferenceRemarks,
        georeference.getDwcGeoreferenceRemarks()));
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
        retrieveAgentNames(identification.getOdsHasAgents(), "identifier")));
    identificationRecord.add(Pair.of(DwcTerm.identifiedByID,
        retrieveAgentIds(identification.getOdsHasAgents(), "identifier")));
    identificationRecord.add(Pair.of(DwcTerm.dateIdentified,
        identification.getDwcDateIdentified()));
    identificationRecord.add(Pair.of(DwcTerm.identificationVerificationStatus,
        String.valueOf(identification.getOdsIsVerifiedIdentification())));
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
        retrieveAgentNames(relationship.getOdsHasAgents(), null)));
    relationshipRecord.add(Pair.of(DwcTerm.relationshipEstablishedDate,
        relationship.getDwcRelationshipEstablishedDate() != null
            ? relationship.getDwcRelationshipEstablishedDate().toString() : null));
    relationshipRecord.add(Pair.of(DwcTerm.relationshipRemarks,
        relationship.getDwcRelationshipRemarks()));
    return relationshipRecord;
  }

  @Override
  protected List<String> targetFields() {
    return List.of();
  }
}
