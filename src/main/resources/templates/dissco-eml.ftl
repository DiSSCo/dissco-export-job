<eml:eml xmlns:eml="https://eml.ecoinformatics.org/eml-2.2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="https://eml.ecoinformatics.org/eml-2.2.0 https://rs.gbif.org/schema/eml-gbif-profile/1.3/eml.xsd" packageId="${package_id}" system="https://dissco.eu" scope="system" xml:lang="en">
  <dataset>
    <title>DiSSCo Export Download ${job_id}</title>
    <creator>
      <individualName>
        <surName>DiSSCo Export Job Service</surName>
      </individualName>
      <onlineUrl>https://github.com/DiSSCo/dissco-export-job</onlineUrl>
      <userId>https://doi.org/10.5281/zenodo.18171542</userId>
      <electronicMailAddress>support@dissco.jitbit.com</electronicMailAddress>
    </creator>
    <metadataProvider>
      <individualName>
        <surName>DiSSCo Export Job Service</surName>
      </individualName>
      <onlineUrl>https://github.com/DiSSCo/dissco-export-job</onlineUrl>
      <userId>https://doi.org/10.5281/zenodo.18171542</userId>
      <electronicMailAddress>support@dissco.jitbit.com</electronicMailAddress>
    </metadataProvider>
    <pubDate>${publication_date}</pubDate>
    <language>ENGLISH</language>
    <abstract>A export containing data from potentially multiple datasets provided by multiple institutions. In total ${number_of_source_systems?c} different source systems provided data</abstract>
    <contact>
      <individualName>
        <surName>DiSSCo Export Job Service</surName>
      </individualName>
      <onlineUrl>https://github.com/DiSSCo/dissco-export-job</onlineUrl>
      <userId>https://doi.org/10.5281/zenodo.18171542</userId>
      <electronicMailAddress>support@dissco.jitbit.com</electronicMailAddress>
    </contact>
  </dataset>
  <additionalMetadata>
    <metadata>
      <gbif>
        <dateStamp>${publication_date_time}</dateStamp>
        <citation identifier="${job_id}">DiSSCo Export Download ${job_id}</citation>
        <physical>
          <objectName>Darwin Core Archive</objectName>
          <characterEncoding>UTF-8</characterEncoding>
          <dataFormat>
            <externallyDefinedFormat>
              <formatName>Darwin Core Archive</formatName>
            </externallyDefinedFormat>
          </dataFormat>
          <distribution>
            <online>
              <url function="download">${export_download_link}</url>
            </online>
          </distribution>
        </physical>
      </gbif>
    </metadata>
  </additionalMetadata>
</eml:eml>