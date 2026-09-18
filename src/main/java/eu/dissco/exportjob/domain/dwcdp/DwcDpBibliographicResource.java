package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpBibliographicResource implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String reference_pk;
  private String referenceID;
  private String isPartOfReference_fk;
  private String isPartOfReferenceID;
  private String referenceType;
  private String bibliographicCitation;
  private String bibliographicIdentifier;
  private String bibliographicIdentifierType;
  private String title;
  private String author;
  private String author_fk;
  private String authorID;
  private String editor;
  private String editor_fk;
  private String editorID;
  private String publisher;
  private String publisher_fk;
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
