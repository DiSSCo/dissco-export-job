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
  private String subjectCategory;
  private String subjectCategoryIRI;
  private String subjectCategorySource;
  private String subjectPartLiteral;
  private String subjectPart;
  private String subjectOrientationLiteral;
  private String subjectOrientation;
  private String physicalSetting;
}
