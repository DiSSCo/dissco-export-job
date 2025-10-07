package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpMaterialUsagePolicy implements Serializable {

  private String usagePolicyID;
  private String materialEntityID;

}
