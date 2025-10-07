package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpRelationship implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String resourceRelationshipID;
  private String subjectResourceID;
  private String subjectResourceType;
  private String subjectResourceTypeIRI;
  private String subjectResourceTypeSource;
  private String relationshipType;
  private String relationshipTypeIRI;
  private String relationshipTypeSource;
  private String relatedResourceID;
  private String externalRelatedResourceID;
  private String externalRelatedResourceSource;
  private String relatedResourceType;
  private String relatedResourceTypeIRI;
  private String relatedResourceTypeSource;
  private Integer relationshipOrder;
  private String relationshipAccordingTo;
  private String relationshipAccordingToID;
  private String relationshipEstablishedDate;
  private String relationshipRemarks;

}
