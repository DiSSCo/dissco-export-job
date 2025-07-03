package eu.dissco.exportjob.component;

import eu.dissco.exportjob.Profiles;
import eu.dissco.exportjob.properties.IndexProperties;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveField;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.TabWriter;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.DwcaTerm;
import org.gbif.dwc.terms.Term;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile(Profiles.DWCA)
public class DwcaZipWriter {

  private static final int CORE_ID_COLUMN = 0;
  private static final Term CORE = DwcTerm.Occurrence;
  private static final Term CORE_ID_TERM = DwcaTerm.ID;
  private static final Archive ARCHIVE = new Archive();
  private static final Map<Term, Pair<TabWriter, Integer>> writers = new HashMap<>();
  @Getter
  private final FileSystem fileSystem;
  private final Template freemarker;

  public DwcaZipWriter(IndexProperties indexProperties,
      @Qualifier("metaTemplate") Template freemarker) throws IOException {
    var zipFile = new File(indexProperties.getTempFileLocation());
    fileSystem = FileSystems.newFileSystem(zipFile.toPath(), Map.of("create", "true"));
    this.freemarker = freemarker;
  }

  private static ArchiveField getIdField() {
    ArchiveField field = new ArchiveField();
    field.setIndex(CORE_ID_COLUMN);
    return field;
  }

  public void writeRecords(Map<Term, List<List<Pair<Term, String>>>> mappedResult)
      throws IOException {
    for (var entry : mappedResult.entrySet()) {
      if (!writers.containsKey(entry.getKey()) && !entry.getValue().isEmpty()) {
        var terms = entry.getValue().getFirst().stream().map(Pair::getLeft).toList();
        var termMap = new HashMap<Term, Integer>();
        for (int i = 0; i < terms.size(); i++) {
          termMap.put(terms.get(i), i);
        }
        createWriter(entry.getKey(), termMap);
      }
      var rowType = entry.getKey();
      var rows = entry.getValue();
      if (rows.isEmpty()) {
        log.debug("Row contains no values for this type: {}", rowType.simpleName());
        continue;
      }
      for (var row : rows) {
        var stringList = row.stream().map(Pair::getRight).toArray(String[]::new);
        try {
          writeRow(rowType, stringList);
        } catch (IOException e) {
          log.error("Failed to write row for type {}: {}", rowType.simpleName(), e.getMessage());
        }
      }
    }
  }

  private void writeMetaFile() throws IOException, TemplateException {
    var metaFile = fileSystem.getPath(Archive.META_FN);
    try (var writer = Files.newBufferedWriter(metaFile, StandardOpenOption.CREATE)) {
      ARCHIVE.setMetadataLocation(Archive.META_FN);
      freemarker.process(ARCHIVE, writer);
    }
  }

  private void writeRow(Term rowType, String[] row) throws IOException {
    var writerInfo = writers.get(rowType);
    var maxMappingColumn = writerInfo.getRight();
    var writer = writerInfo.getLeft();
    if (row.length != maxMappingColumn) {
      throw new IllegalArgumentException(
          "Input rows are not equal to the defined mapping of " + maxMappingColumn + " columns.");
    }
    writer.write(row);
  }

  private void createWriter(Term rowType, Map<Term, Integer> mapping)
      throws IOException {
    if (mapping.isEmpty()) {
      throw new IllegalArgumentException(
          "The writer mapping for term: " + CORE_ID_TERM.simpleName() + " must not be empty.");
    }
    final int maxMapping = maxMappingColumn(mapping);
    TabWriter writer = addArchiveFile(rowType, mapping);
    writers.put(rowType, Pair.of(writer, maxMapping));
  }

  private int maxMappingColumn(Map<Term, Integer> mapping) {
    var addOneForLengthChecking = 1;
    return mapping.values().stream().max(Integer::compareTo).orElseThrow()
        + addOneForLengthChecking;
  }

  private TabWriter addArchiveFile(Term rowType, Map<Term, Integer> mapping)
      throws IOException {
    final Path dataFile = getDataPath(rowType);
    ArchiveFile af = buildArchiveFile(rowType, dataFile);
    for (var entry : mapping.entrySet()) {
      ArchiveField field = new ArchiveField();
      field.setTerm(entry.getKey());
      field.setIndex(entry.getValue());
      af.addField(field);
    }
    if (CORE.equals(rowType)) {
      af.getId().setTerm(CORE_ID_TERM);
      ARCHIVE.setCore(af);
    } else {
      ARCHIVE.addExtension(af);
    }
    return new TabWriter(
        Files.newOutputStream(dataFile, StandardOpenOption.CREATE,
            StandardOpenOption.APPEND));
  }

  private ArchiveFile buildArchiveFile(Term rowType, Path dataFile) {
    ArchiveFile af = ArchiveFile.buildTabFile();
    af.setEncoding("UTF-8");
    af.setRowType(rowType);
    af.addLocation(dataFile.getFileName().toString());
    af.setId(getIdField());
    return af;
  }

  private Path getDataPath(Term rowType) {
    return fileSystem.getPath(rowType.simpleName() + ".tsv");
  }

  public void close() throws IOException, TemplateException {
    writeMetaFile();
    for (var value : writers.values()) {
      value.getLeft().close();
    }
    fileSystem.close();
  }
}
