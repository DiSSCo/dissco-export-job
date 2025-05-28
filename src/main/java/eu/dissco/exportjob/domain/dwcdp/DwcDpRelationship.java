package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpRelationship implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String relationshipID;
  private String subjectResourceID;
  private String subjectResourceType;
  private String subjectResourceTypeIRI;
  private String subjectResourceTypeVocabulary;
  private String relationshipType;
  private String relationshipTypeIRI;
  private String relationshipTypeVocabulary;
  private String relatedResourceID;
  private String relatedResourceType;
  private String relatedResourceTypeIRI;
  private String relatedResourceTypeVocabulary;
  private Integer relationshipOrder;
  private String relationshipAccordingTo;
  private String relationshipAccordingToID;
  private String relationshipEffectiveDate;
  private String relationshipRemarks;

}
