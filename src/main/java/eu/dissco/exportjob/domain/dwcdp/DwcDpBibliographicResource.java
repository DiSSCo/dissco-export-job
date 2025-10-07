package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpBibliographicResource implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String referenceID;
  private String parentReferenceID;
  private String referenceType;
  private String bibliographicCitation;
  private String title;
  private String author;
  private String authorID;
  private String editor;
  private String editorID;
  private String publisher;
  private String publisherID;
  private String volume;
  private String issue;
  private String edition;
  private String pages;
  private String version;
  private String issued;
  private String accessed;
  private Boolean peerReviewStatus;
  private String referenceRemarks;
}
