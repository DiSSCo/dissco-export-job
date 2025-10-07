package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpMaterialUsagePolicy implements Serializable {

  public String usagePolicyID;
  public String materialEntityID;

}
