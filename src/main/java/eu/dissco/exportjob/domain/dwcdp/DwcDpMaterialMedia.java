package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpMaterialMedia implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String mediaID;
  private String materialEntityID;
  private String mediaSubjectCategory;
  private String mediaSubjectCategoryIRI;
  private String mediaSubjectCategoryVocabulary;
}
