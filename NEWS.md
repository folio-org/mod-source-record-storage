## 2024-XX-XX 5.9.4
* [MODSOURCE-817](https://folio-org.atlassian.net/browse/MODSOURCE-817) Fix data consistency in handling and updating Marc Bib records for links.instance-authority event
* [MODSOURCE-834](https://folio-org.atlassian.net/browse/MODSOURCE-834) MARC Indexers Clean up is Taking Too Long
* [MODSOURCE-821](https://folio-org.atlassian.net/browse/MODSOURCE-821) Add System User Support for Eureka env

## 2024-11-27 5.9.3
* [MODSOURCE-824](https://folio-org.atlassian.net/browse/MODSOURCE-824) Endpoint /batch/parsed-records/fetch does not return deleted records
* Update data-import-processing-core to v4.3.1 to fix vulnerabilities

## 2024-11-26 5.9.2
* [MODSOURCE-820](https://folio-org.atlassian.net/browse/MODSOURCE-820) A job with multiple authority match profiles does not work as expected
* [MODSOURCE-816](https://folio-org.atlassian.net/browse/MODSOURCE-816) [RRT] Optimize execution plan for streaming SQL

## 2024-11-21 5.9.1
* [MODSOURCE-826](https://folio-org.atlassian.net/browse/MODSOURCE-826) Fix links update handling to keep order and repeatable subfields

## 2024-10-28 5.9.0
* [MODSOURCE-767](https://folio-org.atlassian.net/browse/MODSOURCE-767) Single record overlay creates duplicate OCLC#/035
* [MODSOURCE-756](https://issues.folio.org/browse/MODSOURCE-756) After setting an instance as marked for deletion it is no longer editable in quickmarc
* [MODSOURCE-753](https://folio-org.atlassian.net/browse/MODSOURCE-753) Change SQL query parameters for MARC Search
* [MODSOURCE-773](https://folio-org.atlassian.net/browse/MODSOURCE-773) MARC Search omits suppressed from discovery records in default search
* [MODINV-1044](https://folio-org.atlassian.net/browse/MODINV-1044) Additional Requirements - Update Data Import logic to normalize OCLC 035 values
* [MODSOURMAN-1200](https://folio-org.atlassian.net/browse/MODSOURMAN-1200) Find record by match id on update generation 
* [MODINV-1049](https://folio-org.atlassian.net/browse/MODINV-1049) Existing "035" field is not retained the original position in imported record 
* [MODSOURCE-785](https://folio-org.atlassian.net/browse/MODSOURCE-785) Update 005 field when set MARC for deletion
* [MODSOURMAN-783](https://folio-org.atlassian.net/browse/MODSOURCE-783) Extend MARC-MARC search query to account for qualifiers
* [MODSOURCE-752](https://folio-org.atlassian.net/browse/MODSOURCE-752) Emit Domain Events For Source Records
* [MODSOURCE-795](https://folio-org.atlassian.net/browse/MODSOURCE-795) Upgrade Spring 5 to 6 by 2024-08-31
* [MODSOURMAN-1203](https://folio-org.atlassian.net/browse/MODSOURMAN-1203) Add validation on MARC_BIB record save
* [MODSOURCE-796](https://folio-org.atlassian.net/browse/MODSOURCE-796) Fix inconsistencies in permission namings
* [MODSOURCE-809](https://folio-org.atlassian.net/browse/MODSOURCE-809) mod-source-record-storage Ramsons 2024 R2 - RMB v35.3.x update
* [MODSOURCE-787](https://folio-org.atlassian.net/browse/MODSOURCE-787) Extend MARC-MARC search query to account for comparison part
* [MODSOURCE-791](https://folio-org.atlassian.net/browse/MODSOURCE-791) Fix Timeout exception during postSourceStorageStreamMarcRecordIdentifiers

## 2024-03-20 5.8.0
* [MODSOURCE-733](https://issues.folio.org/browse/MODSOURCE-733) Reduce Memory Allocation of Strings
* [MODSOURCE-506](https://issues.folio.org/browse/MODSOURCE-506) Remove rawRecord field from source record
* [MODSOURCE-709](https://issues.folio.org/browse/MODSOURCE-709) MARC authority record is not created when use Job profile with match profile by absent subfield/field
* [MODSOURCE-677](https://issues.folio.org/browse/MODSOURCE-677)  Import is completed with errors when control field that differs from 001 is used for marc-to-marc matching
* [MODSOURCE-722](https://issues.folio.org/browse/MODSOURCE-722)  deleteMarcIndexersOldVersions: relation "marc_records_tracking" does not exist
* [MODSOURMAN-1106](https://issues.folio.org/browse/MODSOURMAN-1106) The status of Instance is '-' in the Import log after uploading file. The numbers of updated SRS and Instance are not displayed in the Summary table.
* [MODSOURCE-717](https://issues.folio.org/browse/MODSOURCE-717) MARC modifications not processed when placed after Holdings Update action in a job profile
* [MODSOURCE-739](https://issues.folio.org/browse/MODSOURCE-739) Create Kafka topics instead of relying on auto create in mod-srs
* [MODSOURCE-729](https://issues.folio.org/browse/MODSOURCE-729) Implement new endpoint to be used for matching
* [MODINV-935](https://issues.folio.org/browse/MODINV-935) Move MARC-BIB matching handler to inventory module
* [MODSOURCE-750](https://folio-org.atlassian.net/browse/MODSOURCE-750) Upgrade source-record-storage to RMB 35.2.0, Vert.x 4.5.4
* [MODSOURCE-707](https://folio-org.atlassian.net/browse/MODSOURCE-707) Consume authority domain delete events
* [MODSOURCE-731](https://folio-org.atlassian.net/browse/MODSOURCE-731) Add PUT endpoint to update SRS record
* [MODDATAIMP-957](https://folio-org.atlassian.net/browse/MODDATAIMP-957) Register DI_INCOMING_MARC_BIB_RECORD_PARSED
* [MODSOURCE-732](https://folio-org.atlassian.net/browse/MODSOURCE-732) Change logic of DELETE record endpoint
* [MODINV-967](https://folio-org.atlassian.net/browse/MODINV-967) Move "Modify" action processing to inventory
* [MODINV-935](https://folio-org.atlassian.net/browse/MODINV-935) Move Marc-Bib matching event handler to inventory
* [MODSOURCE-749](https://folio-org.atlassian.net/browse/MODSOURCE-749) 00X fields reset position when Creating/Deriving/Editing MARC records
* [MODSOURCE-608](https://folio-org.atlassian.net/browse/MODSOURCE-608) "PMSystem" displayed as source in "quickmarc" view when record was created by "Non-matches" action of job profile

## 2023-10-13 v5.7.0
* [MODSOURCE-648](https://issues.folio.org/browse/MODSOURCE-648) Upgrade mod-source-record-storage to Java 17
* [MODSOURCE-601](https://issues.folio.org/browse/MODSOURCE-601) Optimize Insert & Update of marc_records_lb table
* [MODSOURCE-635](https://issues.folio.org/browse/MODSOURCE-635) Delete marc_indexers records associated with "OLD" source records
* [MODSOURCE-636](https://issues.folio.org/browse/MODSOURCE-636) Implement async migration service
* [MODSOURCE-674](https://issues.folio.org/browse/MODSOURCE-674) Ensure only one background job can be triggered to clean up outdated marc indexers
* [MODSOURCE-530](https://issues.folio.org/browse/MODSOURCE-530) Fix duplicate records in incoming file causes problems after overlay process with no error reported
* [MODSOURCE-690](https://issues.folio.org/browse/MODSOURCE-690) Make changes in SRS post processing handler to update MARC for shared Instance
* [MODSOURCE-646](https://issues.folio.org/browse/MODSOURCE-646) Make changes to perform MARC To MARC Matching in Local Tenant & Central Tenant
* [MODSOURCE-667](https://issues.folio.org/browse/MODSOURCE-667) Upgrade folio-kafka-wrapper to 3.0.0 version

### Asynchronous migration job API
| METHOD | URL                                     | DESCRIPTION                                     |
|--------|-----------------------------------------|-------------------------------------------------|
| POST   | /source-storage/migrations/jobs         | Initialize asynchronous migration job           |
| GET    | /source-storage/migrations/jobs/{jobId} | Get asynchronous migration job entity by its id |

## 2023-03-18 v5.6.2
* [MODSOURCE-585](https://issues.folio.org/browse/MODSOURCE-585) Data import matching takes incorrect SRS records into consideration
* [MODDATAIMP-786](https://issues.folio.org/browse/MODDATAIMP-786) Update data-import-util library to v1.11.0

## 2023-02-17 v5.6.0
* [MODSOURCE-551](https://issues.folio.org/browse/MODSOURCE-551) Link update: Implement mechanism of topic creation
* [MODSOURCE-557](https://issues.folio.org/browse/MODSOURCE-557) Logging improvement - Configuration
* [MODSOURCE-465](https://issues.folio.org/browse/MODSOURCE-465) Logging improvement
* [MODDATAIMP-722](https://issues.folio.org/browse/MODDATAIMP-722) MARC-to-MARC Holdings update stacked
* [MODDATAIMP-736](https://issues.folio.org/browse/MODDATAIMP-736) Adjust logging configuration for SRS to display datetime in a proper format
* [MODSOURCE-552](https://issues.folio.org/browse/MODSOURCE-552) Extend GET /source-storage/source-records/<Id> to fetch deleted record
* [MODSOURCE-566](https://issues.folio.org/browse/MODSOURCE-566) Improve logging in DAO
* [MODSOURCE-550](https://issues.folio.org/browse/MODSOURCE-550) Link update: Update MARC bibs according to links.instance-authority event
* [MODSOURCE-569](https://issues.folio.org/browse/MODSOURCE-569) Fix transformation to marc record
* [MODSOURCE-571](https://issues.folio.org/browse/MODSOURCE-571) links.instance-authority event not being consumed
* [MODSOURCE-573](https://issues.folio.org/browse/MODSOURCE-573) Linked bib update fails because of snapshot absence
* [MODDATAIMP-758](https://issues.folio.org/browse/MODDATAIMP-758) Improve logging (hide SQL requests)
* [MODSOURCE-578](https://issues.folio.org/browse/MODSOURCE-578) MARC bib field moved to the end of the fields lists after user updates controlling MARC authority record
* [MODSOURCE-567](https://issues.folio.org/browse/MODSOURCE-567) Link update report: send event if update failed
* [MODSOURCE-562](https://issues.folio.org/browse/MODSOURCE-562) Handle bib-authority link when user updates a bib record via data import
* [MODSOURCE-577](https://issues.folio.org/browse/MODSOURCE-577) Prevent Instance PostProcessing Handler from sending DI_COMPLETED
* [MODSOURCE-583](https://issues.folio.org/browse/MODSOURCE-583) Order subfields when update linked subfields
* [MODSOURCE-587](https://issues.folio.org/browse/MODSOURCE-587) Cleanup kafka headers for marc.bib event
* [MODSOURCE-586](https://issues.folio.org/browse/MODSOURCE-586) Fix unable to update "MARC Bib" record upon data import
* [MODDATAIMP-750](https://issues.folio.org/browse/MODDATAIMP-750) Update util libraries dependencies

## 2022-10-19 v5.5.0
* [MODSOURCE-538](https://issues.folio.org/browse/MODSOURCE-538) Subscribe the module to item updated event.
* [MODSOURCE-540](https://issues.folio.org/browse/MODSOURCE-540) Upgrade to RMB v35.0.1
* [MODSOURCE-542](https://issues.folio.org/browse/MODSOURCE-542) scala 2.13.9, kafkaclients 3.1.2, httpclient 4.5.13 fixing vulns
* [MODSOURCE-554](https://issues.folio.org/browse/MODSOURCE-554) Change registration module in tests
* [MODSOURCE-507](https://issues.folio.org/browse/MODSOURCE-507) Reduce Computation of MARC4J Records in AdditionalFieldsUtil
* [MODSOURCE-516](https://issues.folio.org/browse/MODSOURCE-516) Support MARC-MARC Holdings update action

## 2022-09-02 v5.4.2
* [MODDICORE-248](https://issues.folio.org/browse/MODDICORE-248) MARC field protections apply to MARC modifications of incoming records when they should not

## 2022-08-25 v5.4.1
* [MODSOURCE-528](https://issues.folio.org/browse/MODSOURCE-528) Data Import Updates should add 035 field from 001/003, if it's not HRID or already exists
* [MODSOURCE-531](https://issues.folio.org/browse/MODSOURCE-531) Can't update "MARC" record, which was created by stopped import job.
* [MODSOURCE-535](https://issues.folio.org/browse/MODSOURCE-535) Upgrade folio-di-support, folio-liquibase-util, PostgreSQL, Vert.x
* [MODSOURCE-534](https://issues.folio.org/browse/MODSOURCE-534) Allow matching on created 035

## 2022-06-27 v5.4.0
* [MODSOURCE-470](https://issues.folio.org/browse/MODSOURCE-470) Fix permission definition for updating records
* [MODSOURCE-459](https://issues.folio.org/browse/MODSOURCE-459) Determine how to handle deleted authority records
* [MODSOURMAN-724](https://issues.folio.org/browse/MODSOURMAN-724) SRM does not process and save error records
* [MODSOURCE-477](https://issues.folio.org/browse/MODSOURCE-477) Configure job to delete authority records
* [MODSOURCE-447](https://issues.folio.org/browse/MODSOURCE-447) 035 created from 001/003 is not working in SRS record when using a MARC Modification action in Data Import Job Profile
* [MODSOURCE-349](https://issues.folio.org/browse/MODSOURCE-349) Function set_id_in_jsonb is present in migrated Juniper/Kiwi environment
* [MODSOURCE-496](https://issues.folio.org/browse/MODSOURCE-496) Single record imports show the incorrect record number in the summary log 1st column
* [MODSOURMAN-779](https://issues.folio.org/browse/MODSOURMAN-779) Add "CANCELLED" status for Import jobs that are stopped by users.
* [MODSOURCE-499](https://issues.folio.org/browse/MODSOURCE-499) Alleviate Eventloop Blocking During Batch Save of Records
* [MODSOURMAN-801](https://issues.folio.org/browse/MODSOURMAN-801) Inventory Single Record Import: Overlays for Source=MARC Instances retain 003 when they shouldn't
* [MODSOURCE-495](https://issues.folio.org/browse/MODSOURCE-495) Logs show incorrectly formatted request id
* [MODSOURCE-509](https://issues.folio.org/browse/MODSOURCE-509) Data Import Updates should add 035 field from 001/003, if it's not HRID or already exists
* [MODSOURCE-531](https://issues.folio.org/browse/MODSOURCE-531) Can't update "MARC" record, which was created by stopped import job.

## 2022-06-02 v5.3.3
* [MODSOURCE-508](https://issues.folio.org/browse/MODSOURCE-508) Inventory Single Record Import: Overlays for Source=MARC Instances retain 003 when they shouldn't

## 2022-04-04 v5.3.2
* [MODDATAIMP-645](https://issues.folio.org/browse/MODDATAIMP-645) Fixed update a MARC authority record multiple times
* [MODSOURCE-489](https://issues.folio.org/browse/MODSOURCE-489) Records that are overlaid multiple times via Inventory Single Record Import can't be overlaid after the second time
* [MODSOURCE-447](https://issues.folio.org/browse/MODSOURCE-447) 035 created from 001/003 is not working in SRS record when using a MARC Modification action in Data Import Job Profile
* [MODSOURCE-349](https://issues.folio.org/browse/MODSOURCE-349) Remove unused function set_id_in_jsonb

## 2022-03-25 v5.3.1
* [MODSOURCE-482](https://issues.folio.org/browse/MODSOURCE-482) One-record OCLC (Create) Data Import takes over 9 seconds
* [MODSOURCE-470](https://issues.folio.org/browse/MODSOURCE-470) Fix permission definition for updating records
* [MODSOURMAN-724](https://issues.folio.org/browse/MODSOURMAN-724) SRM does not process and save error records

## 2021-02-22 v5.3.0
* [MODSOURCE-461](https://issues.folio.org/browse/MODSOURCE-461) Upgrade RMB and Vertx versions that contain fixes for the connection pool
* [MODSOURCE-420](https://issues.folio.org/browse/MODSOURCE-420) The support for 'maxPoolSize' (DB_MAXPOOLSIZE) from RMB was added
* [MODSOURCE-419](https://issues.folio.org/browse/MODSOURCE-419) Upgrade to RAML Module Builder 33.2.x
* [MODDATAIMP-419](https://issues.folio.org/browse/MODDATAIMP-419) Fix "'idx_records_matched_id_gen', duplicate key value violates unique constraint"
* [MODSOURCE-261](https://issues.folio.org/browse/MODSOURCE-261) Cover kafka handlers with tests and fix ignored
* [MODSOURCE-387](https://issues.folio.org/browse/MODSOURCE-387) Optimistic locking: mod-source-record-storage modifications
* [MODSOURCE-290](https://issues.folio.org/browse/MODSOURCE-290) Implement ProcessRecordErrorHandler for Kafka Consumers
* [MODSOURCE-401](https://issues.folio.org/browse/MODSOURCE-401) Left anchored srs api queries don't complete on kiwi bugfest
* [MODSOURCE-402](https://issues.folio.org/browse/MODSOURCE-402) Properly handle DB failures during events processing
* [MODDATAIMP-491](https://issues.folio.org/browse/MODDATAIMP-491) Improve logging to be able to trace the path of each record and file_chunks
* [MODSOURCE-429](https://issues.folio.org/browse/MODSOURCE-429) Authority update: Implement match handler
* [MODSOURCE-434](https://issues.folio.org/browse/MODSOURCE-434) Remove Kafka cache from QuickMarc handlers
* [MODDATAIMP-623](https://issues.folio.org/browse/MODDATAIMP-623) Remove Kafka cache initialization and Maven dependency
* [MODSOURCE-446](https://issues.folio.org/browse/MODSOURCE-446) Extend MARC-MARC matching to 9xx and 0xx fields
* [MODSOURCE-450](https://issues.folio.org/browse/MODSOURCE-450) Increase folio-liquibase-util and liquibase schema version
* [MODSOURCE-438](https://issues.folio.org/browse/MODSOURCE-438) Fix SQL exceptions regarding connection termination when running CREATE import 
* [MODSOURCE-457](https://issues.folio.org/browse/MODSOURCE-457) Delete Authority: Create processor for Delete MARC Authority record

## 2022-02-09 v5.2.8
* [MODSOURCE-438](https://issues.folio.org/browse/MODSOURCE-438) Fix SQL exceptions regarding connection termination when running CREATE import
* [MODSOURCE-452](https://issues.folio.org/browse/MODSOURCE-452) Updated dependencies on liquibase-util, data-import-processing-core and folio-kafka-wrapper

## 2022-01-13 v5.2.7
* [MODSOURCE-444](https://issues.folio.org/browse/MODSOURCE-444) Fix schema migration error

## 2021-12-15 v5.2.6
* [MODSOURCE-424](https://issues.folio.org/browse/MODSOURCE-424) Fix Log4j vulnerability

## 2021-11-22 v5.2.5
* [MODSOURCE-415](https://issues.folio.org/browse/MODSOURCE-415) Fix processing of DI_ERROR messages from SRS

## 2021-11-19 v5.2.4
* [MODSOURCE-413](https://issues.folio.org/browse/MODSOURCE-413) Update data-import-processing-core to v3.2.5 to enable fix of duplicating values of repeatable control fields on MARC update

## 2021-11-16 v5.2.3
* [MODSOURCE-407](https://issues.folio.org/browse/MODSOURCE-407) Fix potential OOM

## 2021-11-11 v5.2.2
* [MODSOURCE-401](https://issues.folio.org/browse/MODSOURCE-401) Left anchored srs api queries don't complete on kiwi bugfest

## 2021-10-29 v5.2.1
* [MODSOURCE-399](hhttps://issues.folio.org/browse/MODSOURCE-399) Fix "'idx_records_matched_id_gen', duplicate key value violates unique constraint"
* [MODSOURCE-390](https://issues.folio.org/browse/MODSOURCE-390) Fix the effect of DI_ERROR messages when trying to duplicate records on the import job progress bar
* [MODSOURCE-393](https://issues.folio.org/browse/MODSOURCE-393) Enable fix of duplicate control fields on MARC update (update data-import-processing-core to v3.2.2)

## 2021-09-30 v5.2.0
* [MODSOURCE-347](https://issues.folio.org/browse/MODSOURCE-347) Upgrade to RAML Module Builder 33.x
* [MODSOURCE-351](https://issues.folio.org/browse/MODSOURCE-351) Endpoint to verify invalid MARC Bib ids in the system
* [MODSOURCE-342](https://issues.folio.org/browse/MODSOURCE-342) Add HoldingsPostProcessingEventHandler
* [MODSOURCE-326](https://issues.folio.org/browse/MODSOURCE-326) /source-storage/records?limit=0 returns "totalRecords": 0
* [MODSOURCE-286](https://issues.folio.org/browse/MODSOURCE-286) Remove zipping mechanism for data import event payloads and use cache for params
* [MODSOURCE-382](https://issues.folio.org/browse/MODSOURCE-382) Remove dependency on SRM client
* [MODSOURCE-385](https://issues.folio.org/browse/MODSOURCE-385) Remove dependency on converter-storage client
* [MODSOURMAN-515](https://issues.folio.org/browse/MODSOURMAN-515) Error log for unknown event type
* [MODSOURCE-341](https://issues.folio.org/browse/MODSOURCE-341) Store MARC Holdings record
* [MODSOURCE-332](https://issues.folio.org/browse/MODSOURCE-332) Improve performance of left-anchored SRS queries
* [MODSOURCE-343](https://issues.folio.org/browse/MODSOURCE-343) Support MARC Holdings in API

## 2021-09-08 v5.1.6
* [MODSOURCE-368](https://issues.folio.org/browse/MODSOURCE-368) Fix "fill-instance-hrid" script for envs where it was applied before
* [MODSOURCE-360](https://issues.folio.org/browse/MODSOURCE-360) The DB type MARC did not get updated to MARC_BIB

## 2021-09-02 v5.1.5
* [MODSOURCE-357](https://issues.folio.org/browse/MODSOURCE-357) Improve "fill-instance-hrid" script for avoid failing if invalid data exists.

## 2021-08-04 v5.1.4
* [MODSOURCE-345](https://issues.folio.org/browse/MODSOURCE-345) Error log for unknown event type
* [MODPUBSUB-187](https://issues.folio.org/browse/MODPUBSUB-187) Add support for max.request.size configuration for Kafka messages
* Update data-import-processing-core dependency to v3.1.4
* Update folio-kafka-wrapper dependency to v2.3.3

## 2021-07-21 v5.1.3
* [MODSOURCE-329](https://issues.folio.org/browse/MODSOURCE-329) Create script to clean up Snapshot statuses in mod-source-record-storage
* [MODSOURMAN-451](https://issues.folio.org/browse/MODSOURMAN-451) Log details for Inventory single record imports for Overlays
* [MODSOURMAN-508](https://issues.folio.org/browse/MODSOURMAN-508) Log details for Inventory single record imports for Overlays - Part 2
* Update data-import-processing-core dependency to v3.1.3

## 2021-06-25 v5.1.2
* [MODSOURCE-323](https://issues.folio.org/browse/MODSOURCE-323) Change dataType to have common type for MARC related subtypes
* Update data-import-processing-core dependency to v3.1.2

## 2021-06-22 v5.1.1
* [MODSOURCE-311](https://issues.folio.org/browse/MODSOURCE-311) Search API: Restrict to search only by marc bib

## 2021-06-11 v5.1.0
* [MODSOURCE-265](https://issues.folio.org/browse/MODSOURCE-265) Implement presence or absence searches
* [MODSOURCE-269](https://issues.folio.org/browse/MODSOURCE-269) Implement presence or absence searches for indicators
* [MODSOURCE-276](https://issues.folio.org/browse/MODSOURCE-276) Add existing records to the SRS Query API table
* [MODSOURCE-288](https://issues.folio.org/browse/MODSOURCE-288) Migrate QM-flow to Kafka
* [MODSOURCE-279](https://issues.folio.org/browse/MODSOURCE-279) Store MARC Authority record
* [MODSOURCE-308](https://issues.folio.org/browse/MODSOURCE-308) Update interfaces version
* [MODSOURCE-310](https://issues.folio.org/browse/MODSOURCE-310) Handling 001/003/035 handling in SRS for MARC bib records broken
* [MODSOURCE-318](https://issues.folio.org/browse/MODSOURCE-318) Set processingStartedDate for snapshot when updating via QM
* [MODSOURCE-301](https://issues.folio.org/browse/MODSOURCE-301) Cannot import GOBI EDIFACT invoice
* [MODSOURCE-316](https://issues.folio.org/browse/MODSOURCE-316) quickMARC Latency: quickMARC updates are not reflected on Inventory instance record

## 2021-xx-xx v5.0.6
* [MODSOURCE-329](https://issues.folio.org/browse/MODSOURCE-329) Create script to clean up Snapshot statuses in mod-source-record-storage

## 2021-06-17 v5.0.5
* [MODSOURCE-318](https://issues.folio.org/browse/MODSOURCE-318) Set processingStartedDate for snapshot when updating via QM
* [MODSOURCE-310](https://issues.folio.org/browse/MODSOURCE-310) Handling 001/003/035 handling in SRS for MARC bib records broken
* [MODSOURCE-301](https://issues.folio.org/browse/MODSOURCE-301) Cannot import GOBI EDIFACT invoice
* [MODSOURCE-265](https://issues.folio.org/browse/MODSOURCE-265) Implement presence or absence searches
* Update folio-kafka-wrapper to v2.0.8
* Update data-import-processing-core to v3.0.3

## 2021-05-22 v5.0.4
* [MODSOURCE-280](https://issues.folio.org/browse/MODSOURCE-280) Issue with database migration for Iris release
* [MODSOURCE-278](https://issues.folio.org/browse/MODSOURCE-278) Take into account saved records from failed data import on generation calculation. Prevent import hanging if records saving failed
* [MODSOURCE-285](https://issues.folio.org/browse/MODSOURCE-285) Order of execution keeps marc updates from occurring
* [MODSOURCE-295](https://issues.folio.org/browse/MODSOURCE-295) Multiple records are found by match criteria after subsequent updates

## 2021-04-23 v5.0.3
* [MODSOURCE-272](https://issues.folio.org/browse/MODSOURCE-272) MARC srs query API returns 0 results on bugfest

## 2021-04-22 v5.0.2
* [MODSOURCE-267](https://issues.folio.org/browse/MODSOURCE-267) View Source does not update when Data Import updates bib record
* [MODSOURMAN-437](https://issues.folio.org/browse/MODSOURMAN-437) Add logging for event correlationId

## 2021-04-09 v5.0.1
* [MODSOURCE-222](https://issues.folio.org/browse/MODSOURCE-222) Design API & implement data structures and end-points for search functionality in mod-source-record-storage
* [MODSOURCE-223](https://issues.folio.org/browse/MODSOURCE-223) Design and implement service layer for MARC search functionality
* [MODSOURCE-255](https://issues.folio.org/browse/MODSOURCE-255) Return HTTP response according to the schema
* [MODSOURCE-257](https://issues.folio.org/browse/MODSOURCE-257) Implement the search by position
* [MODSOURCE-256](https://issues.folio.org/browse/MODSOURCE-256) Implement the search by the date range
* [MODSOURCE-258](https://issues.folio.org/browse/MODSOURCE-258) Implement NOT searches
* [MODSOURCE-260](https://issues.folio.org/browse/MODSOURCE-260) Position count should begin at 0 when searching control fields
* [MODSOURCE-263](https://issues.folio.org/browse/MODSOURCE-263) SRS MARC Bib column indicates the MARC record was "Created" for "Update" operation

## 2021-03-12 v5.0.0
* [MODSOURMAN-385](https://issues.folio.org/browse/MODSOURMAN-385) Enable OCLC update processing.
* [MODSOURCE-217](https://issues.folio.org/browse/MODSOURCE-217) If incoming MARC bib lacks 001, sometimes it is added and sometimes not.[BUGFIX].
* [MODSOURCE-218](https://issues.folio.org/browse/MODSOURCE-218) Performance issues with SRS requests when instance ID is not found
* [MODSOURCE-232](https://issues.folio.org/browse/MODSOURCE-232) Making Instance Records Suppress from Discovery with the Batch Import is not reflected in the SRS
* [MODSOURCE-220](https://issues.folio.org/browse/MODSOURCE-220) Migration script between Goldenrod-hotfix-5 and Honeysuckle.
* [MODSOURCE-233](https://issues.folio.org/browse/MODSOURCE-233) Support for storing EDIFACT records in the database
* [MODSOURCE-216](https://issues.folio.org/browse/MODSOURCE-216) Update MARC 005 field when MARC record has changes
* [MODSOURCE-204](https://issues.folio.org/browse/MODSOURCE-204) Join query for get records. Add records_lb index for order column.
* [MODSOURCE-203](https://issues.folio.org/browse/MODSOURCE-203) Ensure queries with offset include order. Add records_lb order index to liquibase change log.
* [MODSOURCE-202](https://issues.folio.org/browse/MODSOURCE-202) Records stream API using reactivex.
* [MODSOURCE-201](https://issues.folio.org/browse/MODSOURCE-201) Source Records stream API using reactivex.
* [MODSOURCE-238](https://issues.folio.org/browse/MODSOURCE-238) Use docker-maven-plugin to support build with newer versions of postgres.
* [MODSOURCE-242](https://issues.folio.org/browse/MODSOURCE-242) Use Testcontainers for tests.
* [MODSOURCE-234](https://issues.folio.org/browse/MODSOURCE-234) Add `recordType` query parameter to Records and Source Records APIs. Update join queries to condition on `record_type`. Add database index for `record_type` column of `records_lb` table.
* [MODSOURCE-205](https://issues.folio.org/browse/MODSOURCE-205) Batch API implementation redesign.
* [MODSOURCE-177](https://issues.folio.org/browse/MODSOURCE-177) Use kafka to receive chunks of parsed records for saving
* [MODSOURCE-225](https://issues.folio.org/browse/MODSOURCE-225) Add personal data disclosure.
* [MODSOURCE-225](https://issues.folio.org/browse/MODSOURCE-225) Add personal data disclosure.
* [MODSOURCE-239](https://issues.folio.org/browse/MODSOURCE-239) Upgrade to RAML Module Builder 32.x.
* [MODSOURCE-250](https://issues.folio.org/browse/MODSOURCE-250) Make tenant API asynchronous.
* [MODSOURCE-248](https://issues.folio.org/browse/MODSOURCE-248) Incoming MARC Bib with 003, but no 001 should not create an 035[BUGFIX].


### Stream Records API
 | METHOD |             URL                                      | DESCRIPTION                                                               |
 |--------|------------------------------------------------------|---------------------------------------------------------------------------|
 | GET    | /source-storage/stream/records                       | Stream collection of records; including raw, parsed, and error record     |
 | GET    | /source-storage/stream/source-records                | Stream collection of source records; latest generation with parsed record |

## 2020-11-20 v4.1.3
* [MODSOURCE-212](https://issues.folio.org/browse/MODSOURCE-212) Fix matching by 999 ff s field

## 2020-10-22 v4.1.2
* [MODSOURCE-200](https://issues.folio.org/browse/MODSOURCE-200) Post to /source-storage/batch/records returns "null value in column \"description\" violates not-null constraint" [BUGFIX]
* [MODSOURCE-174](https://issues.folio.org/browse/MODSOURCE-174) Disable CQL2PgJSON & CQLWrapper extra logging in mod-source-record-storage.
* [MODSOURCE-207](https://issues.folio.org/browse/MODSOURCE-207) Upgrade to RMB v31.1.5
* [MODSOURCE-199](https://issues.folio.org/browse/MODSOURCE-199) Store ParsedRecord content as json instead of string

## 2020-10-09 v4.1.1
* [MODSOURMAN-361](https://issues.folio.org/browse/MODSOURMAN-361) Add capability to remove jobs that are stuck

## 2020-10-09 v4.1.0
* [MODSOURMAN-340](https://issues.folio.org/browse/MODSOURMAN-340) MARC field sort into numerical order when record is imported
* [MODSOURMAN-345](https://issues.folio.org/browse/MODSOURMAN-345) 003 handling in SRS for MARC Bib records: Create
* [MODDICORE-58](https://issues.folio.org/browse/MODDICORE-58) MARC-MARC matching for 001 and 999 ff fields
* [MODSOURCE-187](https://issues.folio.org/browse/MODSOURCE-187) Upgrade MODSOURCE to RMB31.0.2
* [MODSOURCE-189](https://issues.folio.org/browse/MODSOURCE-189) Upgrade MODSOURCE to JDK 11
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
