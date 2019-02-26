## 2019-02-12 v1.2.0-SNAPSHOT
* Changed project structure to contain server and client parts. Client builds as a lightweight java library.
* Added a non-production endpoint to populate MARC records for testing purposes, which is available only in case "test.mode" environment variable is set to true.
* Renamed entities sourceRecord -> rawRecord, result -> sourceRecord.
* Updated sourceRecord to contain rawRecord as well as parsedRecord.
* Renamed endpoints
* Sample source records was added. To install the sample data during module initialization, you need to add a parameter to the TenantAttributes with the key "loadSample" and the value "true"
* Used folio-di-support library for Spring Dependency Injection

 | METHOD |             URL                            | DESCRIPTION                                             |
 |--------|--------------------------------------------|---------------------------------------------------------|
 | GET    | /source-storage/snapshots                  | Get list of snapshots                                   |
 | POST   | /source-storage/snapshots                  | Create new snapshot                                     |
 | UPDATE | /source-storage/snapshots/{jobExecutionId} | Update snapshot                                         |
 | GET    | /source-storage/snapshots/{jobExecutionId} | Get snapshot by JobExecution id                         |
 | DELETE | /source-storage/snapshots/{jobExecutionId} | Delete snapshot by JobExecution id                      |
 | GET    | /source-storage/records                    | Get list of records                                     |
 | POST   | /source-storage/records                    | Create new record                                       |
 | UPDATE | /source-storage/records/{id}               | Update record                                           |
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
 | UPDATE | /source-storage/snapshot/{jobExecutionId} | Update snapshot                                         |
 | GET    | /source-storage/snapshot/{jobExecutionId} | Get snapshot by JobExecution id                         |
 | DELETE | /source-storage/snapshot/{jobExecutionId} | Delete snapshot by JobExecution id                      |
 | GET    | /source-storage/record                    | Get list of records                                     |
 | POST   | /source-storage/record                    | Create new record                                       |
 | UPDATE | /source-storage/record/{id}               | Update record                                           |
 | GET    | /source-storage/record/{id}               | Get record by id                                        |
 | DELETE | /source-storage/record/{id}               | Delete record by id                                     |
 | GET    | /source-storage/result                    | Get list of results                                     |
