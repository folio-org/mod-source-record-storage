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
