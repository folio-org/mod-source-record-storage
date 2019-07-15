## 2019-06-12 v2.5.0
* Changed url of updating parsed records from "/source-storage/parsedRecordsCollection" to "/source-storage/batch/parsed-records"
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
