package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpOccurrence implements Serializable {
  
  @Serial
  private static final long serialVersionUID = 1L;
  private String occurrence_pk;
  private String occurrenceID;
  private String event_fk;
  private String isPartOfOccurrence_fk;
  private String occurrenceProtocol_fk;
  private String surveyTarget_fk;
  private String recordNumber;
  private String organismQuantity;
  private String organismQuantityType;
  private String sex;
  private String lifeStage;
  private String reproductiveCondition;
  private String caste;
  private String behavior;
  private String vitality;
  private String establishmentMeans;
  private String degreeOfEstablishment;
  private String pathway;
  private String substrate;
  private String occurrenceStatus;
  private String occurrenceReferences;
  private String occurrenceRemarks;
  private String organism_fk;
  private String organismID;
  private String organismScope;
  private String organismName;
  private String causeOfDeath;
  private String organismRemarks;
  private String verbatimIdentification;
  private String identifiedBy;
  private String identifiedBy_fk;
  private String identifiedByID;
  private String dateIdentified;
  private String identificationReferences;
  private String identificationVerificationStatus;
  private String identificationRemarks;
  private String taxonID;
  private String scientificNameID;
  private String scientificName;
  private String scientificNameAuthorship;
  private String vernacularName;
  private String taxonRank;
  private String classificationSystem;
  private String informationWithheld;
  private String dataGeneralizations;
  private String feedbackURL;

  public boolean isEmpty() {
    return
        (isPartOfOccurrence_fk == null || isPartOfOccurrence_fk.isBlank()) &&
        (occurrenceProtocol_fk == null || occurrenceProtocol_fk.isBlank()) &&
        (surveyTarget_fk == null || surveyTarget_fk.isBlank()) &&
        (recordNumber == null || recordNumber.isBlank()) &&
        (organismQuantity == null || organismQuantity.isBlank()) &&
        (organismQuantityType == null || organismQuantityType.isBlank()) &&
        (sex == null || sex.isBlank()) &&
        (lifeStage == null || lifeStage.isBlank()) &&
        (reproductiveCondition == null || reproductiveCondition.isBlank()) &&
        (caste == null || caste.isBlank()) &&
        (behavior == null || behavior.isBlank()) &&
        (vitality == null || vitality.isBlank()) &&
        (establishmentMeans == null || establishmentMeans.isBlank()) &&
        (degreeOfEstablishment == null || degreeOfEstablishment.isBlank()) &&
        (pathway == null || pathway.isBlank()) &&
        (substrate == null || substrate.isBlank()) &&
        (occurrenceStatus == null || occurrenceStatus.isBlank()) &&
        (occurrenceReferences == null || occurrenceReferences.isBlank()) &&
        (informationWithheld == null || informationWithheld.isBlank()) &&
        (dataGeneralizations == null || dataGeneralizations.isBlank()) &&
        (occurrenceRemarks == null || occurrenceRemarks.isBlank()) &&
        (organism_fk == null || organism_fk.isBlank()) &&
        (organismID == null || organismID.isBlank()) &&
        (organismScope == null || organismScope.isBlank()) &&
        (organismName == null || organismName.isBlank()) &&
        (causeOfDeath == null || causeOfDeath.isBlank()) &&
        (organismRemarks == null || organismRemarks.isBlank()) &&
        (verbatimIdentification == null || verbatimIdentification.isBlank()) &&
        (identifiedBy == null || identifiedBy.isBlank()) &&
        (identifiedBy_fk == null || identifiedBy_fk.isBlank()) &&
        (identifiedByID == null || identifiedByID.isBlank()) &&
        (dateIdentified == null || dateIdentified.isBlank()) &&
        (identificationReferences == null || identificationReferences.isBlank()) &&
        (identificationVerificationStatus == null || identificationVerificationStatus.isBlank()) &&
        (identificationRemarks == null || identificationRemarks.isBlank()) &&
        (taxonID == null || taxonID.isBlank()) &&
        (scientificNameID == null || scientificNameID.isBlank()) &&
        (scientificName == null || scientificName.isBlank()) &&
        (scientificNameAuthorship == null || scientificNameAuthorship.isBlank()) &&
        (vernacularName == null || vernacularName.isBlank()) &&
        (taxonRank == null || taxonRank.isBlank()) &&
        (classificationSystem == null || classificationSystem.isBlank()) &&
        (feedbackURL == null || feedbackURL.isBlank());
  }
}
