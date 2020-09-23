## 2020-08-18 v4.1.0-SNAPSHOT
* [MODSOURMAN-340](https://issues.folio.org/browse/MODSOURMAN-340) MARC field sort into numerical order when record is imported
* [MODSOURMAN-345](https://issues.folio.org/browse/MODSOURMAN-345) 003 handling in SRS for MARC Bib records: Create
* [MODDICORE-58](https://issues.folio.org/browse/MODDICORE-58) MARC-MARC matching for 001 and 999 ff fields
* [MODDATAIMP-324](https://issues.folio.org/browse/MODDATAIMP-324) Update all Data-Import modules to the new RMB version
* [MODSOURCE-184](https://issues.folio.org/browse/MODSOURCE-184) Enable Action profile action for incoming MARC Bibs: UPDATE MARC Bib in SRS 
* [MODSOURCE-186](https://issues.folio.org/browse/MODSOURCE-186) Job status is "Completed with errors" when holding updating.


## 2020-06-25 v4.0.0
* [MODSOURCE-134](https://issues.folio.org/browse/MODSOURCE-134) New Source Record Storage API RAML and updated module descriptor
* [MODSOURCE-143](https://issues.folio.org/browse/MODSOURCE-143) Refactor using jOOQ
* [MODSOURCE-136](https://issues.folio.org/browse/MODSOURCE-136) New API
* [MODSOURCE-144](https://issues.folio.org/browse/MODSOURCE-144) Remove Old API, liquibase migration scripts, drop old tables and functions
* [MODSOURCE-139](https://issues.folio.org/browse/MODSOURCE-139) Upgrade RMB to 30.2.0
* [MODSOURCE-155](https://issues.folio.org/browse/MODSOURCE-155) MARC leader 05 status on record and query parameter to filter by
* [MODSOURCE-157](https://issues.folio.org/browse/MODSOURCE-157) Replace get source records filter by deleted with filter by record state
* [MODSOURCE-156](https://issues.folio.org/browse/MODSOURCE-156) Get source records from list of ids
* [MODSOURCE-159](https://issues.folio.org/browse/MODSOURCE-159) Fix issue with generated client not using query params
* [MODSOURCE-161](https://issues.folio.org/browse/MODSOURCE-161) Remove sample data

### Snapshots API
 | METHOD |             URL                                      | DESCRIPTION                                             |
 |--------|------------------------------------------------------|---------------------------------------------------------|
 | GET    | /source-storage/snapshots                            | Get list of snapshots                                   |
 | POST   | /source-storage/snapshots                            | Create new snapshot                                     |
 | PUT    | /source-storage/snapshots/{jobExecutionId}           | Update snapshot                                         |
 | GET    | /source-storage/snapshots/{jobExecutionId}           | Get snapshot by JobExecution id                         |
 | DELETE | /source-storage/snapshots/{jobExecutionId}           | Delete snapshot and all records by JobExecution id      |

### Records API
 | METHOD |             URL                                      | DESCRIPTION                                             |
 |--------|------------------------------------------------------|---------------------------------------------------------|
 | GET    | /source-storage/records                              | Get list of records                                     |
 | POST   | /source-storage/records                              | Create new record                                       |
 | PUT    | /source-storage/records/{id}                         | Update record                                           |
 | GET    | /source-storage/records/{id}                         | Get record by id                                        |
 | GET    | /source-storage/records/{id}/formatted               | Get formatted record by sourceRecordId or by instanceId |
 | PUT    | /source-storage/records/{id}/suppress-from-discovery | Change suppress from discovery flag for record          |
 | DELETE | /source-storage/records/{id}                         | Set record state to DELETED                             |

### Source Records API
 | METHOD |             URL                                      | DESCRIPTION                                             |
 |--------|------------------------------------------------------|---------------------------------------------------------|
 | GET    | /source-storage/source-records                       | Get list of source records                              |
 | POST   | /source-storage/source-records                       | Get list of source records from list of ids             |
 | GET    | /source-storage/source-records/{id}                  | Get source record by sourceRecordId or by instanceId    |

### Batch Records API
 | METHOD |             URL                                      | DESCRIPTION                                             |
 |--------|------------------------------------------------------|---------------------------------------------------------|
 | POST   | /source-storage/batch/records                        | Create new records                                      |
 | PUT    | /source-storage/batch/parsed-records                 | Update a collection of ParsedRecords                    |

### Event Handlers API
 | METHOD |             URL                                      | DESCRIPTION                                             |
 |--------|------------------------------------------------------|---------------------------------------------------------|
 | POST   | /source-storage/handlers/inventory-instance          | Update record instance id from inventory instance event |
 | POST   | /source-storage/handlers/updated-record              | Crete updated record with generation under new snapshot |

### Test Records API
 | METHOD |             URL                                      | DESCRIPTION                                             |
 |--------|------------------------------------------------------|---------------------------------------------------------|
 | POST   | /source-storage/populate-test-marc-records           | Fill db with test marc records                          |

## 2020-06-10 v3.2.0
* [MODSOURCE-124](https://issues.folio.org/browse/MODSOURCE-124) Apply archive/unarchive eventPayload mechanism.
* [MODSOURCE-115](https://issues.folio.org/browse/MODSOURCE-115) Create endpoint for retrieving "sourceRecords" using "matchedId" and generationId-mechanism.
* [MODSOURCE-129](https://issues.folio.org/browse/MODSOURCE-129) Add Liquibase schema management using [folio-liquibase-util](https://github.com/folio-org/folio-liquibase-util)
* [MODSOURCE-116](https://issues.folio.org/browse/MODSOURCE-116) Add filtering of sourceRecords to return only latest generation.
* [MODSOURCE-130](https://issues.folio.org/browse/MODSOURCE-130) Standard table creation using Liquibase
* [MODSOURCE-102](https://issues.folio.org/browse/MODSOURCE-102) Add Data Access Objects for standard tables
* [MODDATAIMP-300](https://issues.folio.org/browse/MODDATAIMP-300) Updated marc4j version to 2.9.1
* [MODSOURCE-101](https://issues.folio.org/browse/MODSOURCE-101) Add Services for standard table DAOs
* [MODDICORE-50](https://issues.folio.org/browse/MODDICORE-50) Fixed placement of newly-created 035 field
* [MODSOURCE-137](https://issues.folio.org/browse/MODSOURCE-137) Port existing service logic to standard table DAO services
* [MODSOURCE-145](https://issues.folio.org/browse/MODSOURCE-145) Remove matchedProfileId property from Record
* [MODINV-243](https://issues.folio.org/browse/MODINV-243) Enable Action profile action for incoming MARC Bibs: Replace Inventory Instance

## 2020-04-23 v3.1.4
* [MODSOURCE-114](https://issues.folio.org/browse/MODSOURCE-114) Added State "field" to records table
* [MODSOURCE-126](https://issues.folio.org/browse/MODSOURCE-126) Add State "field" to migration script
* [MODOAIPMH-119](https://issues.folio.org/browse/MODOAIPMH-119) Extended sourceRecord schema with externalIdsHolder field
* [MODDICORE-43](https://issues.folio.org/browse/MODDICORE-43) SRS MARC Bib: Fix formatting of 035 field constructed from incoming 001
* [MODSOURCE-123](https://issues.folio.org/browse/MODSOURCE-123) Fixed script to rename pk columns

## 2020-04-07 v3.1.3
* Fixed processing of DataImportEventPayload
* [MODSOURCE-117](https://issues.folio.org/browse/MODSOURCE-117) Added migration script to remove deprecated indexes

## 2020-03-27 v3.1.2
* Added Instance id values to sample data
* Fixed GET source-storage/sourceRecords endpoint to allow filtering by recordId
* Updated reference on ramls/raml-storage

## 2020-03-13 v3.1.1
* Updated reference on ramls/raml-storage

## 2020-03-06 v3.1.0
* Updated RMB to version 29.1.5
* Added module registration as publisher/subscriber to mod-pubsub
* Added condition for generation calculate
* Implemented event handler that updates instance id for MARC bib records

## 2020-01-03 v3.0.2
* Added records metadata filling for batch api

## 2019-12-17 v3.0.1
* Fixed timeout on GET /source-storage/sourceRecords
* Changed default container memory allocation

## 2019-12-04 v3.0.0
* Added migration script to fill externalIdHolder.instanceId fields in records table
* Applied new JVM features to manage container memory

## 2019-11-04 v2.7.0
* Changed payload of endpoint for updating parsed records from ParsedRecordCollection to RecordCollection
* Added order number to the record
* Fixed sorting records by order number
* Broken down source-record-storage interface into smaller ones: source-storage-snapshots, source-storage-records, source-storage-test-records, source-storage-suppress-discovery.

## 2019-09-09 v2.6.0
* Fixed security vulnerability with jackson databind
* Changed response status on partial success of batch save/update - return 201/200 if at least one of the records was saved/updated, 500 if none of the records was saved/updated
* Refactored DAOs to return saved/updated entities
* Added suppress from discovery endpoint for change records value
* Added delete records by jobExecution id endpoint
* Added delete method to the handlers for _tenant interface
* Filled in "fromModuleVersion" value for each "tables" and "scripts" section in schema.json

| METHOD |             URL                                       | DESCRIPTION                                      |
|--------|-------------------------------------------------------|--------------------------------------------------|
| PUT    | /source-storage/record/suppressFromDiscovery          | Change suppress from discovery flag for record   |
| DELETE | /source-storage/snapshots/{jobExecutionId}/records    | Delete records and snapshot by JobExecution id   |

## 2019-07-23 v2.5.0
* Added endpoint for updating parsed records
* Moved endpoint for updating parsed records into the new API interface for batch operations
(URL is changed from '/parsedRecordsCollection' to '/batch/parsed-records')
* Added error message list to ParsedRecords DTO

 | METHOD |             URL                            | DESCRIPTION                                             |
 |--------|--------------------------------------------|---------------------------------------------------------|
 | GET    | /source-storage/snapshots                  | Get list of snapshots                                   |
 | POST   | /source-storage/snapshots                  | Create new snapshot                                     |
 | PUT    | /source-storage/snapshots/{jobExecutionId} | Update snapshot                                         |
 | GET    | /source-storage/snapshots/{jobExecutionId} | Get snapshot by JobExecution id                         |
 | DELETE | /source-storage/snapshots/{jobExecutionId} | Delete snapshot by JobExecution id                      |
 | GET    | /source-storage/records                    | Get list of records                                     |
 | POST   | /source-storage/records                    | Create new record                                       |
 | PUT    | /source-storage/records/{id}               | Update record                                           |
 | GET    | /source-storage/records/{id}               | Get record by id                                        |
 | DELETE | /source-storage/records/{id}               | Mark record deleted                                     |
 | GET    | /source-storage/sourceRecords              | Get list of source records                              |
 | POST   | /source-storage/populate-test-marc-records | Fill db with test marc records                          |
 | POST   | /source-storage/batch/records              | Create new records                                      |
 | PUT    | /source-storage/batch/parsed-records       | Update a collection of ParsedRecords                    |
 | GET    | /source-storage/formattedRecords/{id}      | Get Formatted Record by sourceRecordId or by instanceId |

## 2019-06-12 v2.4.0
* Removed ParsedRecord and ErrorRecord id overriding on save.
* Added endpoint to update a collection of ParsedRecords.
* Added support for ParsedRecord.content represented in json.
* Added endpoint for getting a formatted Record either by sourceRecordId or instanceId
* Moved endpoint for saved record collection from /source-storage/recordsCollection to /source-storage/batch/records 
* Changed response. List of error messages was added that is filled if some record was not saved

 | METHOD |             URL                            | DESCRIPTION                                             |
 |--------|--------------------------------------------|---------------------------------------------------------|
 | GET    | /source-storage/snapshots                  | Get list of snapshots                                   |
 | POST   | /source-storage/snapshots                  | Create new snapshot                                     |
 | PUT    | /source-storage/snapshots/{jobExecutionId} | Update snapshot                                         |
 | GET    | /source-storage/snapshots/{jobExecutionId} | Get snapshot by JobExecution id                         |
 | DELETE | /source-storage/snapshots/{jobExecutionId} | Delete snapshot by JobExecution id                      |
 | GET    | /source-storage/records                    | Get list of records                                     |
 | POST   | /source-storage/records                    | Create new record                                       |
 | PUT    | /source-storage/records/{id}               | Update record                                           |
 | GET    | /source-storage/records/{id}               | Get record by id                                        |
 | DELETE | /source-storage/records/{id}               | Mark record deleted                                     |
 | GET    | /source-storage/sourceRecords              | Get list of source records                              |
 | POST   | /source-storage/populate-test-marc-records | Fill db with test marc records                          | 
 | POST   | /source-storage/batch/records              | Create new records                                      |
 | PUT    | /source-storage/parsedRecordsCollection    | Update a collection of ParsedRecords                    |
 | GET    | /source-storage/formattedRecords/{id}      | Get Formatted Record by sourceRecordId or by instanceId |

## 2019-05-17 v2.3.2
* Added generation of rawRecord id only if it is null.

## 2019-05-16 v2.3.1
* Fixed sorting on GET /source-storage/sourceRecords.

## 2019-05-10 v2.3.0
* Fixed indexes creation for Source Records.
* Optimized performance for a records search.

## 2019-05-03 v2.2.0
* Updated parsedRecord.content and errorRecord.content to contain an object instead of String.
* Updated saving and updating a Record using PreparedStatement.
* Fixed issues with sample data population.
* Added "suppressDiscovery" field for Records.
* Changed delete logic for Records. Records are being marked as deleted and not deleted permanently from storage.
* Added endpoint to create new records from records collection.
* Improved performance for querying sourceRecords on GET /source-storage/sourceRecords.

 | METHOD |             URL                            | DESCRIPTION                                             |
 |--------|--------------------------------------------|---------------------------------------------------------|
 | GET    | /source-storage/snapshots                  | Get list of snapshots                                   |
 | POST   | /source-storage/snapshots                  | Create new snapshot                                     |
 | PUT    | /source-storage/snapshots/{jobExecutionId} | Update snapshot                                         |
 | GET    | /source-storage/snapshots/{jobExecutionId} | Get snapshot by JobExecution id                         |
 | DELETE | /source-storage/snapshots/{jobExecutionId} | Delete snapshot by JobExecution id                      |
 | GET    | /source-storage/records                    | Get list of records                                     |
 | POST   | /source-storage/records                    | Create new record                                       |
 | PUT    | /source-storage/records/{id}               | Update record                                           |
 | GET    | /source-storage/records/{id}               | Get record by id                                        |
 | DELETE | /source-storage/records/{id}               | Mark record deleted                                     |
 | GET    | /source-storage/sourceRecords              | Get list of source records                              |
 | POST   | /source-storage/populate-test-marc-records | Fill db with test marc records                          | 
 | POST   | /source-storage/recordsCollection          | Create new records                                      |

## 2019-03-25 2.1.1
* Updated tenant API version.
* Removed IMPORT_IN_PROGRESS and IMPORT_FINISHED statuses for Snapshot entity.

## 2019-03-20 v2.1.0
* Implemented calculation of generation numbers for Records.

## 2019-02-26 v2.0.0
* Changed project structure to contain server and client parts. Client builds as a lightweight java library.
* Used folio-di-support library for Spring Dependency Injection.
* Added a non-production endpoint to populate MARC records for testing purposes, which is available only in case "loadSample" parameter of TenantAttributes is set to true.
* Renamed entities sourceRecord -> rawRecord, result -> sourceRecord.
* Updated sourceRecord to contain rawRecord as well as parsedRecord.
* Added sample sourceRecords. Sample data is populated during module initialization only in case "loadSample" parameter of TenantAttributes is set to true.
* Changed logging configuration to slf4j.
* Renamed endpoints

 | METHOD |             URL                            | DESCRIPTION                                             |
 |--------|--------------------------------------------|---------------------------------------------------------|
 | GET    | /source-storage/snapshots                  | Get list of snapshots                                   |
 | POST   | /source-storage/snapshots                  | Create new snapshot                                     |
 | PUT    | /source-storage/snapshots/{jobExecutionId} | Update snapshot                                         |
 | GET    | /source-storage/snapshots/{jobExecutionId} | Get snapshot by JobExecution id                         |
 | DELETE | /source-storage/snapshots/{jobExecutionId} | Delete snapshot by JobExecution id                      |
 | GET    | /source-storage/records                    | Get list of records                                     |
 | POST   | /source-storage/records                    | Create new record                                       |
 | PUT    | /source-storage/records/{id}               | Update record                                           |
 | GET    | /source-storage/records/{id}               | Get record by id                                        |
 | DELETE | /source-storage/records/{id}               | Delete record by id                                     |
 | GET    | /source-storage/sourceRecords              | Get list of source records                              |
 | POST   | /source-storage/populate-test-marc-records | Fill db with test marc records                          |
 
## 2018-11-29 v1.0.0
* Created API for managing Snapshot, Record and Result entities

 | METHOD |             URL                           | DESCRIPTION                                             |
 |--------|-------------------------------------------|---------------------------------------------------------|
 | GET    | /source-storage/snapshot                  | Get list of snapshots                                   |
 | POST   | /source-storage/snapshot                  | Create new snapshot                                     |
 | PUT    | /source-storage/snapshot/{jobExecutionId} | Update snapshot                                         |
 | GET    | /source-storage/snapshot/{jobExecutionId} | Get snapshot by JobExecution id                         |
 | DELETE | /source-storage/snapshot/{jobExecutionId} | Delete snapshot by JobExecution id                      |
 | GET    | /source-storage/record                    | Get list of records                                     |
 | POST   | /source-storage/record                    | Create new record                                       |
 | PUT    | /source-storage/record/{id}               | Update record                                           |
 | GET    | /source-storage/record/{id}               | Get record by id                                        |
 | DELETE | /source-storage/record/{id}               | Delete record by id                                     |
 | GET    | /source-storage/result                    | Get list of results                                     |
