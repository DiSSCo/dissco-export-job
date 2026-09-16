package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpMaterialUsagePolicy implements Serializable {

  private String materialEntity_fk;
  private String usagePolicy_fk;

}
