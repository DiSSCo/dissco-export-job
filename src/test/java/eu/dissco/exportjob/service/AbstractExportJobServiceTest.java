package eu.dissco.exportjob.service;

import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.web.ExporterBackendClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AbstractExportJobServiceTest {
  private AbstractExportJobService service;

  @Mock
  private ElasticSearchRepository elasticSearchRepository;
  @Mock
  private ExporterBackendClient exporterBackendClient;

  @BeforeEach
  void init(){
    //service = new AbstractExportJobService(elasticSearchRepository, exporterBackendClient);
  }


}
