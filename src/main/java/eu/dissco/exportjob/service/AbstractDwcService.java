package eu.dissco.exportjob.service;

import eu.dissco.exportjob.component.JobRequestComponent;
import eu.dissco.exportjob.domain.JobRequest;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.properties.IndexProperties;
import eu.dissco.exportjob.properties.S3Properties;
import eu.dissco.exportjob.repository.ElasticSearchRepository;
import eu.dissco.exportjob.repository.S3Repository;
import eu.dissco.exportjob.repository.SourceSystemRepository;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;

import static eu.dissco.exportjob.utils.ExportUtils.removeProxy;

@Slf4j
@Service
public abstract class AbstractDwcService extends AbstractExportJobService {
    private final SourceSystemRepository sourceSystemRepository;
    @Qualifier(value = "emlTemplate")
    private final Template emlTemplate;
    private final S3Properties s3Properties;


    protected AbstractDwcService(ElasticSearchRepository elasticSearchRepository, IndexProperties indexProperties, JsonMapper mapper, JobRequestComponent jobRequestComponent, S3Repository s3Repository, Environment environment, SourceSystemRepository sourceSystemRepository, Template emlTemplate, S3Properties s3Properties) {
        super(elasticSearchRepository, indexProperties, mapper, jobRequestComponent, s3Repository, environment);
        this.sourceSystemRepository = sourceSystemRepository;
        this.emlTemplate = emlTemplate;
        this.s3Properties = s3Properties;
    }

    protected String writeDiSSCoEml(JobRequest jobRequest, FileSystem fs, int numberOfSourceSystem) throws FailedProcessingException {
        var eml = generateEmlFile(jobRequest, numberOfSourceSystem);
        var sourceSystemFile = fs.getPath("eml.xml");
        try {
            Files.writeString(sourceSystemFile, eml, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new FailedProcessingException("Failed to write DiSSCo EML to zip file", e);
        }
        return eml;
    }

    private String generateEmlFile(JobRequest jobRequest, int numberOfSourceSystem) throws FailedProcessingException {
        var now = Instant.now();
        var date = DATE_FORMATTER.format(now);
        var templateMap = new HashMap<String, Object>();
        templateMap.put("job_id", jobRequest.jobId());
        templateMap.put("publication_date", date);
        templateMap.put("publication_date_time", DATE_TIME_FORMATTER.format(now));
        templateMap.put("number_of_source_systems", numberOfSourceSystem);
        templateMap.put("package_id", UUID.randomUUID().toString());
        templateMap.put("export_download_link",
                String.format("https://%s.s3.eu-west-2.amazonaws.com/%s/%s.zip",
                        s3Properties.getBucketName(), date, jobRequest.jobId()));
        var writer = new StringWriter();
        try {
            emlTemplate.process(templateMap, writer);
        } catch (TemplateException | IOException e) {
            throw new FailedProcessingException("Failed to generate the DiSSCo EML file", e);
        }
        return writer.toString();
    }

    protected void writeEmlFileForSourceSystem(String sourceSystemId, FileSystem fs)
            throws FailedProcessingException, IOException {
        log.info("Retrieving EML for source system ID: {}", sourceSystemId);
        var eml = sourceSystemRepository.getEmlBySourceSystemId(sourceSystemId);
        Files.createDirectories(fs.getPath("dataset"));
        var sourceSystemFile = fs.getPath("dataset",
                removeProxy(sourceSystemId).replace('/', '-').toLowerCase() + ".xml");
        Files.writeString(sourceSystemFile, eml, StandardCharsets.UTF_8);
    }

    protected String writeEmlFile(JobRequest jobRequest, FileSystem fs)
            throws FailedProcessingException, IOException {
        var sourceSystemOptional = jobRequest.searchParams().stream()
                .filter(param -> param.inputField().contains("ods:sourceSystemID"))
                .findFirst();
        if (sourceSystemOptional.isEmpty()) {
            throw new FailedProcessingException(
                    "Is a source system job, but no sourceSystemID provided: " + jobRequest.jobId());
        }
        var sourceSystemId = sourceSystemOptional.get().inputValue();
        log.info("Retrieving EML for source system ID: {}", sourceSystemId);
        var eml = sourceSystemRepository.getEmlBySourceSystemId(sourceSystemId);
        var sourceSystemFile = fs.getPath("eml.xml");
        Files.writeString(sourceSystemFile, eml, StandardCharsets.UTF_8);
        return eml;
    }
}
