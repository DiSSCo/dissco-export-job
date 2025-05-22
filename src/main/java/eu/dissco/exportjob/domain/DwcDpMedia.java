package eu.dissco.exportjob.domain;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpMedia implements Serializable {
  
  @Serial
  private static final long serialVersionUID = 1L;
  private String mediaID;
  private String mediaType;
  private String accessURI;
  private String webStatement;
  private String format;
  private String rights;
  private String owner;
  private String source;
  private String creator;
  private String creatorID;
  private String createDate;
  private String modified;
  private String mediaLanguage;
  private String mediaDescription;
}
