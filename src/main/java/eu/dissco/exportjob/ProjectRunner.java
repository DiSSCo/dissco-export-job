package eu.dissco.exportjob;

import eu.dissco.exportjob.component.JobRequestComponent;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.service.AbstractExportJobService;
import lombok.AllArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class ProjectRunner implements CommandLineRunner {

  private final AbstractExportJobService exportJobService;
  private final ConfigurableApplicationContext context;
  private final JobRequestComponent jobRequestComponent;

  @Override
  public void run(String... args) throws FailedProcessingException {
    var request = jobRequestComponent.getJobRequest();
    exportJobService.handleMessage(request);
    context.close();
  }
}
