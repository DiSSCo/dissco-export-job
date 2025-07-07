# Export Job
The exprot job repository contains several job that generate an export product.
This repository is closely associated with the exporter-backend which schedules the jobs.
Within this repository you can find multiple jobs, the profiles will show which jobs are available.
The jobs share the AbstractExportJobService class and are triggered through the ProjectRunner.
The exports of the jobs are stored in an S3 bucket and can be downloaded from there (see the S3Repository).

## Source System Jobs
There are some jobs which are specifically for a source system.
The results of these jobs will be available through the source-system endpoint and can be used for example by GBIF.
The difference with other jobs is that we include the original `eml.xml` for the source system.

# Jobs
## DoiList
This job generates a zipped csv file containing just two columns, the DOI of the specimen and the physicalSpecimenID.
It is straightforward, paginates over elastic and retrieves only these two fields and writes the result to a file.

## DWCA
This job generates a Darwin Core Archive (DwC-A) file containing all the specimen data.
It paginates over elastic and retrieves all data, it then retrieve all media associated with the specimen.
For building the DwC-A we use the GBIF dwca-io [library](https://github.com/gbif/dwca-io).
However, we did implement our own DWCA writer (DwcaZipWriter) as the GBIF implementation did not support streaming to a zipped file.
The DwcaZipWriter is based on this [GBIF stream writer](https://github.com/gbif/dwca-io/blob/master/src/main/java/org/gbif/dwc/DwcaStreamWriter.java)
Then we build the DwC-A where it is important that we include all fields in the correct order.
Based on the first records we determine the order of the fields and this will determine the `meta.xml` file.
This is why we use some magic to always include fields that are mapped to a ods:Location even if the location might not be present in the record.

## DwC-DP
This job contains some additional complexity as we need to potentially deduplicate records.
This means that when the job is started we will create some temporary database tables.
Just as the other jobs we will paginate over elatic and retrieve all data, after which we will also collect any media associated with the specimen.
We then parse this to DwC-DP records and we generate any identifiers when they are not present.
These identifiers are essential in creating the linkages between the different files in the DwC-DP.
The generate identifiers are based on the data in the object (essential for deduplication) and we use an MD5 hash.
We then store the records in the temporary database tables with the identifier as key and the records as a binary array (blob).
This steps takes care of the deduplication as we will ignore any records that have the same primary key (on conflict do nothing).
This concludes the processSearchResults step.
We then move to the postProcessResults which retrieve each record from the temporary database tables and writes them to the DwC-DP files.
We then upload this DwC-DP to the S3 bucket.
