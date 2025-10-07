package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwCDpMaterial implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String materialEntityID;
  private String digitalSpecimenID;
  private String eventID;
  private String materialCategory;
  private String discipline;
  private String materialEntityType;
  private String institutionCode;
  private String institutionID;
  private String ownerInstitutionCode;
  private String ownerInstitutionID;
  private String collectionCode;
  private String collectionID;
  private String catalogNumber;
  private String otherCatalogNumbers;
  private String collectedBy;
  private String collectedByID;
  private String objectQuantity;
  private String objectQuantityType;
  private String collectorNumber;
  private String preparations;
  private String disposition;
  private String verbatimLabel;
  private String associatedSequences;
  private String materialReferences;
  private String informationWithheld;
  private String dataGeneralizations;
  private String materialEntityRemarks;
  private String evidenceForOccurrenceID;
  private String derivedFromMaterialEntityID;
  private String derivationEventID;
  private String derivationType;
  private String isPartOfMaterialEntityID;
  private String verbatimIdentification;
  private String typeStatus;
  private String typeDesignationType;
  private String typifiedName;
  private String identifiedBy;
  private String identifiedByID;
  private String dateIdentified;
  private String identificationReferences;
  private String identificationVerificationStatus;
  private String identificationRemarks;
  private String taxonID;
  private String scientificNameID;
  private String geoClassificationCode;
  private String geoName;
  private String scientificName;
  private String scientificNameAuthorship;
  private String vernacularName;
  private String taxonRank;
  private String externalClassificationSource;
  private String feedbackURL;
}
