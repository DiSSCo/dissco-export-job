package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpMediaUsagePolicy implements Serializable {
  
  public String usagePolicyID;
  public String mediaID;

}
