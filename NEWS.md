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
