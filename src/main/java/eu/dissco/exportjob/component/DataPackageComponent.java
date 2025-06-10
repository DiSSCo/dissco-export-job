package eu.dissco.exportjob.component;

import eu.dissco.exportjob.Profiles;
import eu.dissco.exportjob.domain.dwcdp.DwcDpClasses;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile(Profiles.DWC_DP)
@RequiredArgsConstructor
public class DataPackageComponent {

  @Qualifier("dataPackageTemplate")
  private final Template dataPackageTemplate;
  private final XMLInputFactory xmlFactory;

  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
      "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC);


  public String formatDataPackage(String eml, Set<DwcDpClasses> filesContainingRecords)
      throws FailedProcessingException {
    try {
      var templateMap = retrieveTemplateMap(eml, filesContainingRecords);
      var writer = new StringWriter();
      dataPackageTemplate.process(templateMap, writer);
      return writer.toString();
    } catch (XMLStreamException e) {
      throw new FailedProcessingException("Failed to parse EML XML", e);
    } catch (TemplateException | IOException e) {
      throw new FailedProcessingException("Failed to process data package template", e);
    }
  }

  private Map<String, String> retrieveTemplateMap(String eml,
      Set<DwcDpClasses> filesContainingRecords) throws XMLStreamException {
    var templateMap = new HashMap<String, String>();
    var xmlEventReader = xmlFactory.createXMLEventReader(new StringReader(eml));
    while (xmlEventReader.hasNext()) {
      var element = xmlEventReader.nextEvent();
      extractTitle(element, "title", templateMap, xmlEventReader);
      extractTitle(element, "description", templateMap, xmlEventReader);
      extractTitle(element, "licenseName", templateMap, xmlEventReader);
      extractTitle(element, "identifier", templateMap, xmlEventReader);
    }
    templateMap.put("created", formatter.format(Instant.now()));
    filesContainingRecords.forEach(
        fileClass -> templateMap.put(fileClass.getClassName().replace("-", "_"), "true"));
    return templateMap;
  }

  private void extractTitle(XMLEvent element, String templateElement,
      HashMap<String, String> templateMap,
      XMLEventReader xmlEventReader) throws XMLStreamException {
    if (isStartElement(element, templateElement)) {
      templateMap.put(templateElement, xmlEventReader.nextEvent().asCharacters().getData());
    }
  }

  private boolean isStartElement(XMLEvent element, String field) {
    if (element != null) {
      return element.isStartElement() && element.asStartElement().getName().getLocalPart()
          .equals(field);
    } else {
      return false;
    }
  }

}
