package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpIdentificationAgent implements Serializable {
    
    @Serial
    private static final long serialVersionUID = 1L;
    private String agentID;
    private String identificationID;
    private String agentRole;
    private String agentRoleIRI;
    private String agentRoleVocabulary;
    private Integer agentRoleOrder;
    private String agentRoleDate;
}
