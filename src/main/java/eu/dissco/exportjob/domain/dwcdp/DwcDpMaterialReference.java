package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpMaterialReference implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String referenceID;
  private String materialEntityID;
  private String relationshipType;
}
