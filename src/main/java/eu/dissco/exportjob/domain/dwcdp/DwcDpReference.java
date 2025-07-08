package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpReference implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String referenceID;
  private String parentReferenceID;
  private String referenceType;
  private String bibliographicCitation;
  private String title;
  private String issued;
  private String identifier;
  private String creator;
  private String creatorID;
  private String publisher;
  private String publisherID;
  private String pagination;
  private Boolean isPeerReviewed;
  private String referenceRemarks;
}
