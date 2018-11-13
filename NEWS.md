## 2018-11-13 v1.0.3
 * Added schema for Result entity
 * Added results_view to the db
 * Defined endpoint for GETting the results
 * Implemented the endpoint
 * Created API tests for the added endpoint
 
  API for result: 

 | METHOD |             URL             | DESCRIPTION                                             |
 |--------|-----------------------------|---------------------------------------------------------|
 | GET    | /source-storage/result      | Get list of results                                     |

## 2018-11-12 v1.0.2
 * Added schemas for Record entity
 * Added tables for records, source_records, error_records, marc_records and the records_view to the db
 * Defined endpoints for managing CRUD operations over Record entity
 * Implemented the endpoints methods
 * Created API tests for each added endpoint
 
  CRUD API for record: 

 | METHOD |             URL             | DESCRIPTION                                             |
 |--------|-----------------------------|---------------------------------------------------------|
 | GET    | /source-storage/record      | Get list of records                                     |
 | POST   | /source-storage/record      | Create new record                                       |
 | UPDATE | /source-storage/record/{id} | Update record                                           |
 | GET    | /source-storage/record/{id} | Get record by id                                        |
 | DELETE | /source-storage/record/{id} | Delete record by id                                     |

## 2018-10-31 v1.0.1
 * Added schemas for Snapshot entity
 * Added table for snapshots to the db
 * Defined endpoints for managing CRUD operations over Snapshot entity
 * Implemented the endpoints methods
 * Created API tests for each added endpoint
 
  CRUD API for snapshot: 

 | METHOD |             URL                           | DESCRIPTION                                             |
 |--------|-------------------------------------------|---------------------------------------------------------|
 | GET    | /source-storage/snapshot                  | Get list of snapshots                                   |
 | POST   | /source-storage/snapshot                  | Create new snapshot                                     |
 | UPDATE | /source-storage/snapshot/{jobExecutionId} | Update snapshot                                         |
 | GET    | /source-storage/snapshot/{jobExecutionId} | Get snapshot by JobExecution id                         |
 | DELETE | /source-storage/snapshot/{jobExecutionId} | Delete snapshot by JobExecution id                      |

## 2018-10-09 v1.0.0
 * Add endpoints to retrieve source change jobs and logs for that jobs
 * Implementation is currently stubbed
 * Use RestAssured library for REST testing needs
 * Use RAML 1.0
 
  API for jobs and logs: 

 | METHOD |             URL                | DESCRIPTION                                             |
 |--------|------------------------------- |---------------------------------------------------------|
 | GET    | /record-storage/items          | Get list of the source change jobs based on criteria    |
 | GET    | /record-storage/items/{itemId} | Get particular source change job by id                  |
 | DELETE | /record-storage/items/{itemId} | Delete particular source change job by id               |
 | GET    | /record-storage/logs           | Get a list of logs for source records based on criteria |

## 2018-09-24 v0.0.1
 * Initial module setup
