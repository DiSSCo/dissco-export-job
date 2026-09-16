package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpMaterialReference implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String reference_fk;
  private String materialEntity_fk;
  private String relationshipType;
}
