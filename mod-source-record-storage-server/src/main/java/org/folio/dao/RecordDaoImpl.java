package org.folio.dao;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.folio.dao.util.AdvisoryLockUtil.acquireLock;
import static org.folio.dao.util.ErrorRecordDaoUtil.ERROR_RECORD_CONTENT;
import static org.folio.dao.util.ParsedRecordDaoUtil.PARSED_RECORD_CONTENT;
import static org.folio.dao.util.RawRecordDaoUtil.RAW_RECORD_CONTENT;
import static org.folio.dao.util.RecordDaoUtil.RECORD_NOT_FOUND_TEMPLATE;
import static org.folio.dao.util.RecordDaoUtil.ensureRecordForeignKeys;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByMultipleIds;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalIdNonNull;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByState;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByType;
import static org.folio.dao.util.RecordDaoUtil.getExternalHrid;
import static org.folio.dao.util.RecordDaoUtil.getExternalId;
import static org.folio.dao.util.RecordDaoUtil.getExternalIdType;
import static org.folio.dao.util.SnapshotDaoUtil.SNAPSHOT_NOT_FOUND_TEMPLATE;
import static org.folio.dao.util.SnapshotDaoUtil.SNAPSHOT_NOT_STARTED_MESSAGE_TEMPLATE;
import static org.folio.rest.jooq.Tables.ERROR_RECORDS_LB;
import static org.folio.rest.jooq.Tables.MARC_RECORDS_LB;
import static org.folio.rest.jooq.Tables.MARC_RECORDS_TRACKING;
import static org.folio.rest.jooq.Tables.RAW_RECORDS_LB;
import static org.folio.rest.jooq.Tables.RECORDS_LB;
import static org.folio.rest.jooq.Tables.SNAPSHOTS_LB;
import static org.folio.rest.jooq.enums.RecordType.MARC_BIB;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.QueryParamUtil.toRecordType;
import static org.jooq.impl.DSL.condition;
import static org.jooq.impl.DSL.countDistinct;
import static org.jooq.impl.DSL.exists;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.selectDistinct;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.trueCondition;


import com.google.common.collect.Lists;
import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.reactivex.Flowable;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.sqlclient.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.ErrorRecordDaoUtil;
import org.folio.dao.util.IdType;
import org.folio.dao.util.MatchField;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.RawRecordDaoUtil;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.RecordType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.dao.util.TenantUtil;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Filter;
import org.folio.rest.jaxrs.model.MarcBibCollection;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordsBatchResponse;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordIdentifiersDto;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.RecordsIdentifiersCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jaxrs.model.StrippedParsedRecord;
import org.folio.rest.jaxrs.model.StrippedParsedRecordCollection;
import org.folio.rest.jooq.enums.JobExecutionStatus;
import org.folio.rest.jooq.enums.RecordState;
import org.folio.rest.jooq.tables.records.ErrorRecordsLbRecord;
import org.folio.rest.jooq.tables.records.RawRecordsLbRecord;
import org.folio.rest.jooq.tables.records.RecordsLbRecord;
import org.folio.rest.jooq.tables.records.SnapshotsLbRecord;
import org.folio.services.RecordSearchParameters;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.folio.services.entities.RecordsModifierOperator;
import org.folio.services.exceptions.RecordUpdateException;
import org.folio.services.util.TypeConnection;
import org.folio.services.util.parser.ParseFieldsResult;
import org.folio.services.util.parser.ParseLeaderResult;
import org.jooq.CommonTableExpression;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Name;
import org.jooq.OrderField;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.ResultQuery;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.SelectOnConditionStep;
import org.jooq.SortOrder;
import org.jooq.Table;
import org.jooq.TableLike;
import org.jooq.UpdateConditionStep;
import org.jooq.UpdateSetFirstStep;
import org.jooq.UpdateSetMoreStep;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RecordDaoImpl implements RecordDao {

  private static final Logger LOG = LogManager.getLogger();

  private static final String CTE = "cte";

  private static final String ID = "id";
  private static final String MARC_ID = "marc_id";
  private static final String VERSION = "version";
  private static final String HRID = "hrid";
  private static final String CONTENT = "content";
  private static final String COUNT = "count";
  private static final String TABLE_FIELD_TEMPLATE = "{0}.{1}";
  private static final String MARC_INDEXERS_PARTITION_PREFIX = "marc_indexers_";

  private static final int DEFAULT_LIMIT_FOR_GET_RECORDS = 1;
  private static final String UNIQUE_VIOLATION_SQL_STATE = "23505";
  private static final int RECORDS_LIMIT = Integer.parseInt(System.getProperty("RECORDS_READING_LIMIT", "999"));
  static final int INDEXERS_DELETION_LOCK_NAMESPACE_ID = "delete_marc_indexers".hashCode();

  public static final String CONTROL_FIELD_CONDITION_TEMPLATE = "\"{partition}\".\"value\" in ({value})";
  public static final String CONTROL_FIELD_CONDITION_TEMPLATE_WITH_QUALIFIER =
    "\"{partition}\".\"value\" LIKE {qualifier}" +
      " AND {comparisonValue} IN ({value}) ";
  public static final String DATA_FIELD_CONDITION_TEMPLATE = "\"{partition}\".\"value\" in ({value}) and \"{partition}\".\"ind1\" LIKE '{ind1}' and \"{partition}\".\"ind2\" LIKE '{ind2}' and \"{partition}\".\"subfield_no\" = '{subfield}'";
  public static final String DATA_FIELD_CONDITION_TEMPLATE_WITH_QUALIFIER =
    "\"{partition}\".\"value\" LIKE {qualifier} " +
      "AND {comparisonValue} IN ({value}) " +
      "AND \"{partition}\".\"ind1\" LIKE '{ind1}' " +
      "AND \"{partition}\".\"ind2\" LIKE '{ind2}' " +
      "AND \"{partition}\".\"subfield_no\" = '{subfield}'";
  private static final String VALUE_IN_SINGLE_QUOTES = "'%s'";
  private static final String RECORD_NOT_FOUND_BY_ID_TYPE = "Record with %s id: %s was not found";
  private static final String INVALID_PARSED_RECORD_MESSAGE_TEMPLATE = "Record %s has invalid parsed record; %s";
  private static final String WILDCARD = "*";
  private static final String PERCENT = "%";
  private static final String HASH = "#";

  private static final Field<Integer> COUNT_FIELD = field(name(COUNT), Integer.class);

  private static final Field<?>[] RECORD_FIELDS = new Field<?>[]{
    RECORDS_LB.ID,
    RECORDS_LB.SNAPSHOT_ID,
    RECORDS_LB.MATCHED_ID,
    RECORDS_LB.GENERATION,
    RECORDS_LB.RECORD_TYPE,
    RECORDS_LB.EXTERNAL_ID,
    RECORDS_LB.STATE,
    RECORDS_LB.LEADER_RECORD_STATUS,
    RECORDS_LB.ORDER,
    RECORDS_LB.SUPPRESS_DISCOVERY,
    RECORDS_LB.CREATED_BY_USER_ID,
    RECORDS_LB.CREATED_DATE,
    RECORDS_LB.UPDATED_BY_USER_ID,
    RECORDS_LB.UPDATED_DATE,
    RECORDS_LB.EXTERNAL_HRID
  };

  private static final String DELETE_MARC_INDEXERS_TEMP_TABLE = "marc_indexers_deleted_ids";

  public static final String OR = " or ";
  public static final String MARC_INDEXERS = "marc_indexers";
  public static final Field<UUID> MARC_INDEXERS_MARC_ID = field(TABLE_FIELD_TEMPLATE, UUID.class, field(MARC_INDEXERS), field(MARC_ID));
  public static final String CALL_DELETE_OLD_MARC_INDEXERS_VERSIONS_PROCEDURE = "CALL delete_old_marc_indexers_versions()";
  public static final String OLD_RECORDS_TRACKING_TABLE = "old_records_tracking";
  public static final String HAS_BEEN_PROCESSED_FLAG = "has_been_processed";
  public static final String MARC_ID_COLUMN = "marc_id";

  private final PostgresClientFactory postgresClientFactory;
  private final RecordDomainEventPublisher recordDomainEventPublisher;

  @org.springframework.beans.factory.annotation.Value("${srs.record.matching.fallback-query.enable:false}")
  private boolean enableFallbackQuery;

  @Autowired
  public RecordDaoImpl(final PostgresClientFactory postgresClientFactory,
                       final RecordDomainEventPublisher recordDomainEventPublisher) {
    this.postgresClientFactory = postgresClientFactory;
    this.recordDomainEventPublisher = recordDomainEventPublisher;
  }

  @Override
  public <T> Future<T> executeInTransaction(Function<ReactiveClassicGenericQueryExecutor, Future<T>> action, String tenantId) {
    return getQueryExecutor(tenantId).transaction(action);
  }

  @Override
  public Future<RecordCollection> getRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    return getRecords(condition, recordType, orderFields, offset, limit, true, tenantId);
  }

  @Override
  public Future<RecordCollection> getRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields,
                                             int offset, int limit, boolean returnTotalCount, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl ->
      readRecords(dsl, condition, recordType, offset, limit, returnTotalCount, orderFields)
    )).map(queryResult -> toRecordCollectionWithLimitCheck(queryResult, limit));
  }

  @Override
  public Future<StrippedParsedRecordCollection> getStrippedParsedRecords(List<String> externalIds, IdType idType, RecordType recordType, Boolean includeDeleted, String tenantId) {
    Name cte = name(CTE);
    Name prt = name(recordType.getTableName());
    Condition condition;
    if (includeDeleted != null && includeDeleted) {
      condition = RecordDaoUtil.getExternalIdsCondition(externalIds, idType)
        .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL).or(RECORDS_LB.STATE.eq(RecordState.DELETED)));
    } else {
      condition = RecordDaoUtil.getExternalIdsCondition(externalIds, idType)
        .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL));
    }
    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl -> dsl
      .with(cte.as(dsl.selectCount()
        .from(RECORDS_LB)
        .where(condition.and(recordType.getRecordImplicitCondition()))))
      .select(getStrippedParsedRecordWithCount(prt))
      .from(RECORDS_LB)
      .leftJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
      .rightJoin(dsl.select().from(table(cte))).on(trueCondition())
      .where(condition.and(recordType.getRecordImplicitCondition()))
    )).map(this::toStrippedParsedRecordCollection);
  }

  @Override
  public Future<List<Record>> getMatchedRecords(MatchField matchedField, Filter.ComparisonPartType comparisonPartType,
                                                List<String> matchedRecordIds, TypeConnection typeConnection,
                                                boolean externalIdRequired, int offset, int limit, String tenantId) {
    Name prt = name(typeConnection.getDbType().getTableName());
    Table<org.jooq.Record> marcIndexersPartitionTable = table(name(MARC_INDEXERS_PARTITION_PREFIX + matchedField.getTag()));
    if (matchedField.getValue() instanceof MissingValue)
      return Future.succeededFuture(emptyList());

    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl ->
      {
        SelectOnConditionStep<org.jooq.Record> query = dsl
          .select(getAllRecordFields(prt))
          .distinctOn(RECORDS_LB.ID)
          .from(RECORDS_LB)
          .leftJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
          .leftJoin(RAW_RECORDS_LB).on(RECORDS_LB.ID.eq(RAW_RECORDS_LB.ID))
          .leftJoin(ERROR_RECORDS_LB).on(RECORDS_LB.ID.eq(ERROR_RECORDS_LB.ID))
          .innerJoin(marcIndexersPartitionTable).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, marcIndexersPartitionTable, name(MARC_ID))));
        if (typeConnection.getDbType()
          .getTableName().equalsIgnoreCase(MARC_RECORDS_LB.getName())) {
          query = query.innerJoin(MARC_RECORDS_TRACKING)
            .on(MARC_RECORDS_TRACKING.MARC_ID
              .eq(field(TABLE_FIELD_TEMPLATE, UUID.class, marcIndexersPartitionTable, name(MARC_ID)))
              .and(MARC_RECORDS_TRACKING.VERSION
                .eq(field(TABLE_FIELD_TEMPLATE, Integer.class, marcIndexersPartitionTable, name(VERSION)))));
        }
        return query.where(
            filterRecordByType(typeConnection.getRecordType().value())
              .and(filterRecordByMultipleIds(matchedRecordIds))
              .and(filterRecordByState(Record.State.ACTUAL.value()))
              .and(externalIdRequired ? filterRecordByExternalIdNonNull() : DSL.noCondition())
              .and(getMatchedFieldCondition(matchedField, comparisonPartType, marcIndexersPartitionTable.getName()))
          )
          .offset(offset)
          .limit(limit > 0 ? limit : DEFAULT_LIMIT_FOR_GET_RECORDS);
      }
    )).compose(queryResult -> handleMatchedRecordsSearchResult(queryResult, matchedField, comparisonPartType, typeConnection, externalIdRequired, offset, limit, tenantId));
  }

  private Future<List<Record>> handleMatchedRecordsSearchResult(QueryResult queryResult, MatchField matchedField, Filter.ComparisonPartType comparisonPartType,
                                                                TypeConnection typeConnection,
                                                                boolean externalIdRequired, int offset, int limit, String tenantId) {
    if (enableFallbackQuery && !queryResult.hasResults()) {
      return getMatchedRecordsWithoutIndexersVersionUsage(matchedField, comparisonPartType, typeConnection, externalIdRequired, offset, limit, tenantId);
    }
    return Future.succeededFuture(queryResult.stream().map(res -> asRow(res.unwrap())).map(this::toRecord).toList());
  }

  public Future<List<Record>> getMatchedRecordsWithoutIndexersVersionUsage(MatchField matchedField, Filter.ComparisonPartType comparisonPartType,
                                                                           TypeConnection typeConnection, boolean externalIdRequired,
                                                                           int offset, int limit, String tenantId) {
    Name prt = name(typeConnection.getDbType().getTableName());
    Table<org.jooq.Record> marcIndexersPartitionTable = table(name(MARC_INDEXERS_PARTITION_PREFIX + matchedField.getTag()));
    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl -> dsl
      .select(getAllRecordFields(prt))
      .distinctOn(RECORDS_LB.ID)
      .from(RECORDS_LB)
      .leftJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
      .leftJoin(RAW_RECORDS_LB).on(RECORDS_LB.ID.eq(RAW_RECORDS_LB.ID))
      .leftJoin(ERROR_RECORDS_LB).on(RECORDS_LB.ID.eq(ERROR_RECORDS_LB.ID))
      .innerJoin(marcIndexersPartitionTable).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, marcIndexersPartitionTable, name(MARC_ID))))
      .where(
        filterRecordByType(typeConnection.getRecordType().value())
          .and(filterRecordByState(Record.State.ACTUAL.value()))
          .and(externalIdRequired ? filterRecordByExternalIdNonNull() : DSL.noCondition())
          .and(getMatchedFieldCondition(matchedField, comparisonPartType, marcIndexersPartitionTable.getName()))
      )
      .offset(offset)
      .limit(limit > 0 ? limit : DEFAULT_LIMIT_FOR_GET_RECORDS)
    )).map(queryResult -> queryResult.stream()
      .map(res -> asRow(res.unwrap()))
      .map(this::toRecord)
      .toList()
    );
  }

  private Condition getMatchedFieldCondition(MatchField matchedField, Filter.ComparisonPartType comparisonPartType, String partition) {
    Map<String, String> params = new HashMap<>();
    var qualifierSearch = false;
    params.put("partition", partition);
    params.put("value", getValueInSqlFormat(matchedField.getValue()));
    if (matchedField.getQualifierMatch() != null) {
      qualifierSearch = true;
      params.put("qualifier", getSqlQualifier(matchedField.getQualifierMatch()));
    }
    params.put("comparisonValue", getComparisonValue(comparisonPartType));

    String sql;
    if (matchedField.isControlField()) {
      sql = qualifierSearch ? StrSubstitutor.replace(CONTROL_FIELD_CONDITION_TEMPLATE_WITH_QUALIFIER, params, "{", "}")
        : StrSubstitutor.replace(CONTROL_FIELD_CONDITION_TEMPLATE, params, "{", "}");
    } else {
      params.put("ind1", getSqlInd(matchedField.getInd1()));
      params.put("ind2", getSqlInd(matchedField.getInd2()));
      params.put("subfield", matchedField.getSubfield());
      sql = qualifierSearch ? StrSubstitutor.replace(DATA_FIELD_CONDITION_TEMPLATE_WITH_QUALIFIER, params, "{", "}")
        : StrSubstitutor.replace(DATA_FIELD_CONDITION_TEMPLATE, params, "{", "}");
    }
    return condition(sql);
  }

  private static String getComparisonValue(Filter.ComparisonPartType comparisonPartType) {

    String DEFAULT_VALUE = "\"{partition}\".\"value\"";
    if (comparisonPartType == null) {
      return DEFAULT_VALUE;
    }

    return switch (comparisonPartType) {
      //case ALPHANUMERICS_ONLY -> "regexp_replace(\"{partition}\".\"value\", '[^[:alnum:]]', '', 'g')";
      case ALPHANUMERICS_ONLY -> "regexp_replace(\"{partition}\".\"value\", '[^\\w]|_', '', 'g')";
      case NUMERICS_ONLY -> "regexp_replace(\"{partition}\".\"value\", '[^[:digit:]]', '', 'g')";
      default -> DEFAULT_VALUE;
    };
  }

  private String getSqlInd(String ind) {
    if (ind.equals(WILDCARD)) return PERCENT;
    if (ind.isBlank()) return HASH;
    return ind;
  }

  private String getSqlQualifier(MatchField.QualifierMatch qualifierMatch) {
    if (qualifierMatch == null) {
      return null;
    }
    var value = qualifierMatch.value();

    return switch (qualifierMatch.qualifier()) {
      case BEGINS_WITH -> "'" + value + "%'";
      case ENDS_WITH -> "'%" + value + "'";
      case CONTAINS -> "'%" + value + "%'";
    };
  }

  private String getValueInSqlFormat(Value value) {
    if (Value.ValueType.STRING.equals(value.getType())) {
      return format(VALUE_IN_SINGLE_QUOTES, value.getValue());
    }
    if (Value.ValueType.LIST.equals(value.getType())) {
      List<String> listOfValues = ((ListValue) value).getValue().stream()
        .map(v -> format(VALUE_IN_SINGLE_QUOTES, v))
        .toList();
      return StringUtils.join(listOfValues, ", ");
    }
    return StringUtils.EMPTY;
  }

  @Override
  public Flowable<Record> streamRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    Name prt = name(recordType.getTableName());
    String sql = DSL.select(getAllRecordFields(prt))
      .from(RECORDS_LB)
      .leftJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
      .leftJoin(RAW_RECORDS_LB).on(RECORDS_LB.ID.eq(RAW_RECORDS_LB.ID))
      .leftJoin(ERROR_RECORDS_LB).on(RECORDS_LB.ID.eq(ERROR_RECORDS_LB.ID))
      .where(condition.and(recordType.getRecordImplicitCondition()))
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit)
      .getSQL(ParamType.INLINED);

    return getCachedPool(tenantId)
      .rxGetConnection()
      .flatMapPublisher(conn -> conn.rxBegin()
        .flatMapPublisher(tx -> conn.rxPrepare(sql)
          .flatMapPublisher(pq -> pq.createStream(1)
            .toFlowable()
            .map(this::toRow)
            .map(this::toRecord))
          .doAfterTerminate(tx::commit)
          .doOnError(error -> {
            tx.rollback();
            conn.close();
          })
          .doFinally(conn::close)));
  }

  private static String buildCteDistinctCountCondition(Expression expression) {
    StringBuilder combinedExpression = new StringBuilder();
    if (expression instanceof Parenthesis parenthesis) {
      Expression innerExpression = parenthesis.getExpression();
      if (containsParenthesis(innerExpression)) {
        combinedExpression.append(buildCteDistinctCountCondition(innerExpression));
      } else {
        combinedExpression.append(countDistinct(DSL.case_().when(DSL.condition(expression.toString()), 1)).eq(1));
      }
    } else if (expression instanceof BinaryExpression binaryExpression) {
      Expression leftExpression = binaryExpression.getLeftExpression();
      Expression rightExpression = binaryExpression.getRightExpression();
      if (containsParenthesis(leftExpression)) {
        combinedExpression.append("(");
        combinedExpression.append(buildCteDistinctCountCondition(leftExpression));
      }
      if (expression instanceof AndExpression) {
        combinedExpression.append(" and ");
      } else if (expression instanceof OrExpression) {
        combinedExpression.append(OR);
      }
      if (containsParenthesis(rightExpression)) {
        combinedExpression.append(buildCteDistinctCountCondition(rightExpression));
        combinedExpression.append(")");
      }
    }
    return combinedExpression.toString();
  }

  private static void parseExpression(Expression expr, List<Expression> expressions) {
    if (expr instanceof BinaryExpression binExpr) {
      parseExpression(binExpr.getLeftExpression(), expressions);
      parseExpression(binExpr.getRightExpression(), expressions);
    } else if (expr instanceof Parenthesis parenthesis) {
      if (containsParenthesis(parenthesis.getExpression())) parseExpression(parenthesis.getExpression(), expressions);
      else expressions.add(parenthesis);
    }
  }

  private static boolean containsParenthesis(Expression expr) {
    if (expr instanceof Parenthesis) {
      return true;
    } else if (expr instanceof BinaryExpression binExpr) {
      return containsParenthesis(binExpr.getLeftExpression()) || containsParenthesis(binExpr.getRightExpression());
    } else {
      return false;
    }
  }

  private String buildCteWhereCondition(String whereExpression) throws JSQLParserException {
    List<Expression> expressions = new ArrayList<>();
    StringBuilder cteWhereCondition = new StringBuilder();

    Expression expr = CCJSqlParserUtil.parseCondExpression(whereExpression);
    parseExpression(expr, expressions);
    int i = 1;
    for (Expression expression : expressions) {
      cteWhereCondition.append(expression.toString());
      if (i < expressions.size()) cteWhereCondition.append(OR);
      i++;
    }
    return cteWhereCondition.toString();
  }

  @Override
  public Flowable<Row> streamMarcRecordIds(ParseLeaderResult parseLeaderResult, ParseFieldsResult parseFieldsResult,
                                           RecordSearchParameters searchParameters, String tenantId) throws JSQLParserException {
    /* Building a search query */
    //TODO: adjust bracets in condtion statements
    CommonTableExpression commonTableExpression = null;
    if (parseFieldsResult.isEnabled()) {
      String cteWhereExpression = buildCteWhereCondition(parseFieldsResult.getWhereExpression());

      Expression expr = CCJSqlParserUtil.parseCondExpression(parseFieldsResult.getWhereExpression());
      String cteHavingExpression = buildCteDistinctCountCondition(expr);

      commonTableExpression = DSL.name(CTE).as(
        DSL.selectDistinct(MARC_INDEXERS_MARC_ID)
          .from(MARC_INDEXERS)
          .join(MARC_RECORDS_TRACKING).on(MARC_RECORDS_TRACKING.MARC_ID.eq(MARC_INDEXERS_MARC_ID))
          .where(DSL.condition(cteWhereExpression, parseFieldsResult.getBindingParams().toArray()))
          .groupBy(MARC_INDEXERS_MARC_ID)
          .having(DSL.condition(cteHavingExpression, parseFieldsResult.getBindingParams().toArray()))
      );
    }

    SelectJoinStep searchQuery = selectDistinct(RECORDS_LB.EXTERNAL_ID).from(RECORDS_LB);
    appendJoin(searchQuery, parseLeaderResult);
    appendWhere(searchQuery, parseLeaderResult, parseFieldsResult, searchParameters);
    if (searchParameters.getOffset() != null) {
      searchQuery.offset(searchParameters.getOffset());
    }
    if (searchParameters.getLimit() != null) {
      searchQuery.limit(searchParameters.getLimit());
    }
    /* Building a count query */
    SelectJoinStep countQuery = DSL.select(countDistinct(RECORDS_LB.EXTERNAL_ID)).from(RECORDS_LB);
    appendJoin(countQuery, parseLeaderResult);
    appendWhere(countQuery, parseLeaderResult, parseFieldsResult, searchParameters);
    /* Join both in one query */
    String sql = "";
    if (parseFieldsResult.isEnabled()) {
      sql = DSL.with(commonTableExpression).select().from(searchQuery).rightJoin(countQuery).on(DSL.trueCondition()).getSQL(ParamType.INLINED);
    } else {
      sql = select().from(searchQuery).rightJoin(countQuery).on(DSL.trueCondition()).getSQL(ParamType.INLINED);
    }
    String finalSql = sql;
    LOG.trace("streamMarcRecordIds:: SQL : {}", finalSql);
    return getCachedPool(tenantId)
      .rxGetConnection()
      .flatMapPublisher(conn -> conn.rxBegin()
        .flatMapPublisher(tx -> conn.rxPrepare(finalSql)
          .flatMapPublisher(pq -> pq.createStream(10000)
            .toFlowable()
            .map(this::toRow))
          .doAfterTerminate(tx::commit)
          .doOnError(error -> {
            tx.rollback();
            conn.close();
          })
          .doFinally(conn::close)));
  }

  private void appendJoin(SelectJoinStep selectJoinStep, ParseLeaderResult parseLeaderResult) {
    if (parseLeaderResult.isEnabled() && !parseLeaderResult.isIndexedFieldsCriteriaOnly()) {
      Table<org.jooq.Record> marcIndexersLeader = table(name("marc_indexers_leader"));
      selectJoinStep.innerJoin(marcIndexersLeader).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, marcIndexersLeader, name(MARC_ID))));
    }
  }

  private void appendWhere(SelectJoinStep step, ParseLeaderResult parseLeaderResult, ParseFieldsResult parseFieldsResult, RecordSearchParameters searchParameters) {
    Condition recordTypeCondition = RecordDaoUtil.filterRecordByType(searchParameters.getRecordType().value());
    Condition recordStateCondition = RecordDaoUtil.filterRecordByDeleted(searchParameters.isDeleted());
    Condition suppressedFromDiscoveryCondition = RecordDaoUtil.filterRecordBySuppressFromDiscovery(searchParameters.isSuppressedFromDiscovery());
    Condition leaderCondition = parseLeaderResult.isEnabled()
      ? DSL.condition(parseLeaderResult.getWhereExpression(), parseLeaderResult.getBindingParams().toArray())
      : DSL.noCondition();
    Condition fieldsCondition = parseFieldsResult.isEnabled()
      ? exists(select(field("*")).from("cte")
      .where("records_lb.id = cte.marc_id"))
      : DSL.noCondition();
    step.where(leaderCondition)
      .and(fieldsCondition)
      .and(recordStateCondition)
      .and(suppressedFromDiscoveryCondition)
      .and(recordTypeCondition)
      .and(RECORDS_LB.EXTERNAL_ID.isNotNull());
  }

  @Override
  public Future<RecordsIdentifiersCollection> getMatchedRecordsIdentifiers(MatchField matchedField, Filter.ComparisonPartType comparisonPartType,
                                                                           boolean returnTotalRecords, TypeConnection typeConnection,
                                                                           boolean externalIdRequired, int offset, int limit, String tenantId) {
    Table<org.jooq.Record> marcIndexersPartitionTable = table(name(MARC_INDEXERS_PARTITION_PREFIX + matchedField.getTag()));
    if (matchedField.getValue() instanceof MissingValue) {
      return Future.succeededFuture(new RecordsIdentifiersCollection().withTotalRecords(0));
    }

    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl -> {
      TableLike<Record1<Integer>> countQuery;
      if (returnTotalRecords) {
        countQuery = select(countDistinct(RECORDS_LB.ID))
          .from(RECORDS_LB)
          .innerJoin(marcIndexersPartitionTable)
          .on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, marcIndexersPartitionTable, name(MARC_ID))))
          .innerJoin(MARC_RECORDS_TRACKING)
          .on(MARC_RECORDS_TRACKING.MARC_ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, marcIndexersPartitionTable, name(MARC_ID)))
            .and(MARC_RECORDS_TRACKING.VERSION.eq(field(TABLE_FIELD_TEMPLATE, Integer.class, marcIndexersPartitionTable, name(VERSION)))))
          .where(filterRecordByType(typeConnection.getRecordType().value())
            .and(filterRecordByState(Record.State.ACTUAL.value()))
            .and(externalIdRequired ? filterRecordByExternalIdNonNull() : DSL.noCondition())
            .and(getMatchedFieldCondition(matchedField, comparisonPartType, marcIndexersPartitionTable.getName())));
      } else {
        countQuery = select(inline(null, Integer.class).as(COUNT));
      }

      SelectConditionStep<org.jooq.Record> searchQuery = dsl
        .select(List.of(RECORDS_LB.ID, RECORDS_LB.EXTERNAL_ID))
        .distinctOn(RECORDS_LB.ID)
        .from(RECORDS_LB)
        .innerJoin(marcIndexersPartitionTable)
        .on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, marcIndexersPartitionTable, name(MARC_ID))))
        .innerJoin(MARC_RECORDS_TRACKING)
        .on(MARC_RECORDS_TRACKING.MARC_ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, marcIndexersPartitionTable, name(MARC_ID)))
          .and(MARC_RECORDS_TRACKING.VERSION.eq(field(TABLE_FIELD_TEMPLATE, Integer.class, marcIndexersPartitionTable, name(VERSION)))))
        .where(filterRecordByType(typeConnection.getRecordType().value())
          .and(filterRecordByState(Record.State.ACTUAL.value()))
          .and(externalIdRequired ? filterRecordByExternalIdNonNull() : DSL.noCondition())
          .and(getMatchedFieldCondition(matchedField, comparisonPartType, marcIndexersPartitionTable.getName())));

      return DSL.select()
        .from(searchQuery)
        .rightJoin(countQuery).on(DSL.trueCondition())
        .orderBy(searchQuery.field(ID).asc())
        .offset(offset)
        .limit(limit);
    })).map(result -> toRecordsIdentifiersCollection(result, returnTotalRecords));
  }

  private RecordsIdentifiersCollection toRecordsIdentifiersCollection(QueryResult result, boolean returnTotalRecords) {
    Integer countResult = asRow(result.unwrap()).getInteger(COUNT);
    if (returnTotalRecords && (countResult == null || countResult == 0)) {
      return new RecordsIdentifiersCollection().withTotalRecords(0);
    }

    List<RecordIdentifiersDto> identifiers = result.stream()
      .map(res -> asRow(res.unwrap()))
      .map(row -> new RecordIdentifiersDto()
        .withRecordId(row.getUUID(ID).toString())
        .withExternalId(row.getUUID(RECORDS_LB.EXTERNAL_ID.getName()).toString()))
      .toList();

    return new RecordsIdentifiersCollection()
      .withIdentifiers(identifiers)
      .withTotalRecords(countResult);
  }

  @Override
  public Future<Optional<Record>> getRecordById(String id, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordById(txQE, id));
  }

  @Override
  public Future<Optional<Record>> getRecordById(ReactiveClassicGenericQueryExecutor txQE, String id) {
    Condition condition = RECORDS_LB.ID.eq(UUID.fromString(id));
    return getRecordByCondition(txQE, condition);
  }

  @Override
  public Future<Optional<Record>> getRecordByMatchedId(String matchedId, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordByMatchedId(txQE, matchedId));
  }

  @Override
  public Future<Optional<Record>> getRecordByMatchedId(ReactiveClassicGenericQueryExecutor txQE, String id) {
    Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(id))
      .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL)
        .or(RECORDS_LB.STATE.eq(RecordState.DELETED)));
    return getRecordByCondition(txQE, condition);
  }

  @Override
  public Future<Optional<Record>> getRecordByCondition(Condition condition, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordByCondition(txQE, condition));
  }

  @Override
  public Future<Optional<Record>> getRecordByCondition(ReactiveClassicGenericQueryExecutor txQE, Condition condition) {
    return RecordDaoUtil.findByCondition(txQE, condition)
      .compose(record -> lookupAssociatedRecords(txQE, record, true));
  }

  @Override
  public Future<Record> saveRecord(Record record, Map<String, String> okapiHeaders) {
    var tenantId = okapiHeaders.get(OKAPI_TENANT_HEADER);
    LOG.trace("saveRecord:: Saving {} record {} for tenant {}", record.getRecordType(), record.getId(), tenantId);
    return getQueryExecutor(tenantId).transaction(txQE -> saveRecord(txQE, record, okapiHeaders));
  }

  @Override
  public Future<Record> saveRecord(ReactiveClassicGenericQueryExecutor txQE, Record recordDto,
                                   Map<String, String> okapiHeaders) {
    LOG.trace("saveRecord:: Saving {} record {}", recordDto.getRecordType(), recordDto.getId());
    return insertOrUpdateRecord(txQE, recordDto)
      .onSuccess(created -> recordDomainEventPublisher.publishRecordCreated(created, okapiHeaders));
  }

  @Override
  public Future<RecordsBatchResponse> saveRecords(RecordCollection recordCollection, Map<String, String> okapiHeaders) {
    var tenantId = okapiHeaders.get(OKAPI_TENANT_HEADER);
    logRecordCollection("saveRecords :: Saving", recordCollection, tenantId);
    var records = recordCollection.getRecords();
    var firstRecord = records.iterator().next();
    var snapshotId = firstRecord.getSnapshotId();
    var recordType = RecordType.valueOf(firstRecord.getRecordType().name());
    Promise<RecordsBatchResponse> finalPromise = Promise.promise();
    Context context = Vertx.currentContext();

    // make sure only one snapshot id
    var snapshotIdsAreDifferent = records.stream().anyMatch(r -> !Objects.equals(snapshotId, r.getSnapshotId()));
    if (snapshotIdsAreDifferent) {
      throw new BadRequestException("Batch record collection only supports single snapshot");
    }

    if(context == null) {
      return Future.failedFuture("saveRecords must be executed by a Vertx thread");
    }

    context.owner().executeBlocking(
      () -> {
        Set<UUID> matchedIds = new HashSet<>();
        List<RecordsLbRecord> dbRecords = new ArrayList<>();
        List<RawRecordsLbRecord> dbRawRecords = new ArrayList<>();
        List<Record2<UUID, JSONB>> dbParsedRecords = new ArrayList<>();
        List<ErrorRecordsLbRecord> dbErrorRecords = new ArrayList<>();

        extractValidatedDatabasePersistRecords(records, recordType, matchedIds, dbRecords,
          dbRawRecords, dbParsedRecords, dbErrorRecords);
        List<String> errorMessages = getErrorMessages(dbErrorRecords);

        try (Connection connection = getConnection(tenantId)) {
          return DSL.using(connection).transactionResult(ctx -> {
            DSLContext dsl = DSL.using(ctx);

            // validate snapshot
            validateSnapshot(UUID.fromString(snapshotId), ctx);

            // save records
            persistDatabaseRecords(dsl, recordType, matchedIds, dbRecords, dbRawRecords, dbParsedRecords, dbErrorRecords);

            // return result
            return new RecordsBatchResponse()
              .withRecords(records)
              .withTotalRecords(records.size())
              .withErrorMessages(errorMessages);
          });
        } catch (DuplicateEventException e) {
          LOG.info("saveRecords :: Skipped saving records due to duplicate event: {}", e.getMessage());
          throw e;
        } catch (SQLException | DataAccessException ex) {
          LOG.warn("saveRecords :: Failed to save records", ex);
          Throwable throwable = ex.getCause() != null ? ex.getCause() : ex;
          throw new RecordUpdateException(throwable);
        }
      },
      false,
      r -> {
        if (r.failed()) {
          LOG.warn("saveRecords :: Error during batch record save", r.cause());
          finalPromise.fail(r.cause());
        } else {
          LOG.debug("saveRecords :: batch record save was successful");
          finalPromise.complete(r.result());
        }
      });

    return finalPromise.future()
      .onSuccess(response -> response.getRecords()
        .forEach(r -> recordDomainEventPublisher.publishRecordCreated(r, okapiHeaders))
      );
  }

  @Override
  public Future<RecordsBatchResponse> saveRecordsByExternalIds(List<String> externalIds,
                                                               RecordType recordType,
                                                               RecordsModifierOperator recordsModifier,
                                                               Map<String, String> okapiHeaders) {
    var condition = RecordDaoUtil.getExternalIdsCondition(externalIds,
        getExternalIdType(Record.RecordType.fromValue(recordType.name())))
      .and(RecordDaoUtil.filterRecordByDeleted(false));

    var tenantId = okapiHeaders.get(OKAPI_TENANT_HEADER);
    Promise<RecordsBatchResponse> finalPromise = Promise.promise();
    Context context = Vertx.currentContext();
    if(context == null) {
      return Future.failedFuture("saveRecordsByExternalIds :: operation must be executed by a Vertx thread");
    }

    context.owner().executeBlocking(
      () -> {
        try (Connection connection = getConnection(tenantId)) {
          return DSL.using(connection).transactionResult(ctx -> {
            DSLContext dsl = DSL.using(ctx);
            var queryResult = readRecords(dsl, condition, recordType, 0, externalIds.size(), false, emptyList());
            var records = queryResult.fetch(this::toRecord);

            if (CollectionUtils.isEmpty(records)) {
              LOG.warn("saveRecordsByExternalIds :: No records returned from the fetch query");
              return new RecordsBatchResponse().withTotalRecords(0);
            }
            var existingRecordsCollection = new RecordCollection().withRecords(records).withTotalRecords(records.size());
            var modifiedRecords = recordsModifier.apply(existingRecordsCollection);

            // validate snapshot
            var snapshotId = modifiedRecords.getRecords().iterator().next().getSnapshotId();
            var snapshotIdsAreDifferent = records.stream().anyMatch(r -> !Objects.equals(snapshotId, r.getSnapshotId()));
            if (snapshotIdsAreDifferent) {
              throw new BadRequestException("Batch record collection only supports single snapshot");
            }
            validateSnapshot(UUID.fromString(snapshotId), ctx);

            // extract db records to save
            Set<UUID> matchedIds = new HashSet<>();
            List<RecordsLbRecord> dbRecords = new ArrayList<>();
            List<RawRecordsLbRecord> dbRawRecords = new ArrayList<>();
            List<Record2<UUID, JSONB>> dbParsedRecords = new ArrayList<>();
            List<ErrorRecordsLbRecord> dbErrorRecords = new ArrayList<>();

            extractValidatedDatabasePersistRecords(modifiedRecords.getRecords(), recordType, matchedIds, dbRecords,
              dbRawRecords, dbParsedRecords, dbErrorRecords);

            // save records
            LOG.info("saveRecordsByExternalIds :: recordCollection: {}", modifiedRecords.getTotalRecords());
            for (var dbRecord : dbRecords) {
              LOG.info("dbRecord id: {}, state: {}, generation: {}", dbRecord.getId(), dbRecord.getState(), dbRecord.getGeneration());
            }
            persistDatabaseRecords(dsl, recordType, matchedIds, dbRecords, dbRawRecords, dbParsedRecords, dbErrorRecords);

            // return result
            List<String> errorMessages = getErrorMessages(dbErrorRecords);
            queryResult = readRecords(dsl, condition, recordType, 0, externalIds.size(), false, emptyList());
            records = queryResult.fetch(this::toRecord);
            return new RecordsBatchResponse()
              .withRecords(records)
              .withTotalRecords(records.size())
              .withErrorMessages(errorMessages);
          });
        } catch (DuplicateEventException e) {
          LOG.info("saveRecordsByExternalIds :: Skipped saving records due to duplicate event: {}", e.getMessage());
          throw e;
        } catch (SQLException | DataAccessException e) {
          LOG.warn("saveRecordsByExternalIds :: Failed to read and save modified records", e);
          throw e;
        }
      },
      r -> {
        if (r.failed()) {
          LOG.warn("saveRecordsByExternalIds:: Error during batch record save", r.cause());
          finalPromise.fail(r.cause());
        } else {
          LOG.debug("saveRecordsByExternalIds:: batch record save was successful");
          finalPromise.complete(r.result());
        }
      }
    );

    return finalPromise.future()
      .onSuccess(response -> response.getRecords()
        .forEach(r -> recordDomainEventPublisher.publishRecordCreated(r, okapiHeaders))
      );
  }

  private ResultQuery<org.jooq.Record> readRecords(DSLContext dsl, Condition condition, RecordType recordType, int offset, int limit,
                                                   boolean returnTotalCount, Collection<OrderField<?>> orderFields) {
    Name cte = name(CTE);
    Name prt = name(recordType.getTableName());
    var finalCondition = condition.and(recordType.getRecordImplicitCondition());

    ResultQuery<Record1<Integer>> countQuery;
    if (returnTotalCount) {
      countQuery = dsl.selectCount()
        .from(RECORDS_LB)
        .where(finalCondition);
    } else {
      countQuery = select(inline(null, Integer.class).as(COUNT));
    }
    return dsl
      .with(cte.as(countQuery))
      .select(getAllRecordFieldsWithCount(prt))
      .from(RECORDS_LB)
      .leftJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
      .leftJoin(RAW_RECORDS_LB).on(RECORDS_LB.ID.eq(RAW_RECORDS_LB.ID))
      .leftJoin(ERROR_RECORDS_LB).on(RECORDS_LB.ID.eq(ERROR_RECORDS_LB.ID))
      .rightJoin(dsl.select().from(table(cte))).on(trueCondition())
      .where(finalCondition)
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit > 0 ? limit : DEFAULT_LIMIT_FOR_GET_RECORDS);
  }

  private void extractValidatedDatabasePersistRecords(List<Record> records,
                                                      RecordType recordType,
                                                      Set<UUID> matchedIds,
                                                      List<RecordsLbRecord> dbRecords,
                                                      List<RawRecordsLbRecord> dbRawRecords,
                                                      List<Record2<UUID, JSONB>> dbParsedRecords,
                                                      List<ErrorRecordsLbRecord> dbErrorRecords) {
    records.stream()
      .map(RecordDaoUtil::ensureRecordHasId)
      .map(RecordDaoUtil::ensureRecordHasMatchedId)
      .map(RecordDaoUtil::ensureRecordHasSuppressDiscovery)
      .map(RecordDaoUtil::ensureRecordForeignKeys)
      .forEach(record -> {
        // collect unique matched ids to query to determine generation
        matchedIds.add(UUID.fromString(record.getMatchedId()));

        validateRecordType(record, recordType);

        // if record has parsed record, validate by attempting format
        if (Objects.nonNull(record.getParsedRecord())) {
          try {
            recordType.formatRecord(record);
            Record2<UUID, JSONB> dbParsedRecord = recordType.toDatabaseRecord2(record.getParsedRecord());
            dbParsedRecords.add(dbParsedRecord);
          } catch (Exception e) {
            // create error record and remove from record
            Object content = Optional.ofNullable(record.getParsedRecord())
              .map(ParsedRecord::getContent)
              .orElse(null);
            var errorRecord = new ErrorRecord()
              .withId(record.getId())
              .withDescription(e.getMessage())
              .withContent(content);
            record.withErrorRecord(errorRecord)
              .withParsedRecord(null)
              .withLeaderRecordStatus(null);
          }
        }

        if (Objects.nonNull(record.getRawRecord())) {
          dbRawRecords.add(RawRecordDaoUtil.toDatabaseRawRecord(record.getRawRecord()));
        }

        if (Objects.nonNull(record.getErrorRecord())) {
          dbErrorRecords.add(ErrorRecordDaoUtil.toDatabaseErrorRecord(record.getErrorRecord()));
        }

        dbRecords.add(RecordDaoUtil.toDatabaseRecord(record));
      });
  }

  private void persistDatabaseRecords(DSLContext dsl,
                                      RecordType recordType,
                                      Set<UUID> matchedIds,
                                      List<RecordsLbRecord> dbRecords,
                                      List<RawRecordsLbRecord> dbRawRecords,
                                      List<Record2<UUID, JSONB>> dbParsedRecords,
                                      List<ErrorRecordsLbRecord> dbErrorRecords) throws IOException {
    List<UUID> ids = new ArrayList<>();
    //Map<UUID, Integer> matchedGenerations = new HashMap<>();

    // lookup the latest generation by matched id and committed snapshot updated before current snapshot
    dsl.select(RECORDS_LB.MATCHED_ID, RECORDS_LB.ID, RECORDS_LB.GENERATION)
      .distinctOn(RECORDS_LB.MATCHED_ID)
      .from(RECORDS_LB)
      .innerJoin(SNAPSHOTS_LB).on(RECORDS_LB.SNAPSHOT_ID.eq(SNAPSHOTS_LB.ID))
      .where(RECORDS_LB.MATCHED_ID.in(matchedIds)
        .and(SNAPSHOTS_LB.STATUS.in(JobExecutionStatus.COMMITTED, JobExecutionStatus.ERROR, JobExecutionStatus.CANCELLED))
      )
      .orderBy(RECORDS_LB.MATCHED_ID.asc(), RECORDS_LB.GENERATION.desc())
      .fetchStream().forEach(r -> {
        UUID id = r.get(RECORDS_LB.ID);
        //UUID matchedId = r.get(RECORDS_LB.MATCHED_ID);
        //int generation = r.get(RECORDS_LB.GENERATION);
        ids.add(id);
        //matchedGenerations.put(matchedId, generation);
      });

    ids.forEach(id -> LOG.info("Set record with ID to OLD state: {}", id));

    dbRecords.forEach(dbRecord -> {
      var generation = Optional.ofNullable(dbRecord.getGeneration())
        .map(g -> ++g)
        .orElse(0);
      dbRecord.setGeneration(generation);
      LOG.info("dbRecord id: {}, state: {}, new generation: {}", dbRecord.getId(), dbRecord.getState(), dbRecord.getGeneration());
    });
    //LOG.info("persistDatabaseRecords :: dbRecords: {}, matchedGenerations: {}", dbRecords.size(), matchedGenerations.size());

    // update matching records state
    if(CollectionUtils.isNotEmpty(ids)) {
      dsl.update(RECORDS_LB)
        .set(RECORDS_LB.STATE, RecordState.OLD)
        .where(RECORDS_LB.ID.in(ids))
        .execute();
    }

    // batch insert records updating generation if required
    var recordsLoadingErrors = dsl.loadInto(RECORDS_LB)
      .batchAfter(1000)
      .bulkAfter(500)
      .commitAfter(1000)
      .onErrorAbort()
      .loadRecords(dbRecords)
//      .loadRecords(dbRecords.stream()
//        .map(recordDto -> {
//          Integer generation = matchedGenerations.get(recordDto.getMatchedId());
//          recordDto.setGeneration(Objects.nonNull(generation) ? ++generation : 0);
//          LOG.debug("persistDatabaseRecords :: matchedId: {}, set new generation: {}",
//            recordDto.getMatchedId(), generation);
//          return recordDto;
//        })
//        .toList())
      .fieldsCorresponding()
      .execute()
      .errors();

    recordsLoadingErrors.forEach(error -> {
      if (error.exception().sqlState().equals(UNIQUE_VIOLATION_SQL_STATE)) {
        throw new DuplicateEventException("SQL Unique constraint violation prevented repeatedly saving the record");
      }
      LOG.warn("persistDatabaseRecords :: Error occurred on batch execution: {}", error.exception().getCause().getMessage());
      LOG.debug("persistDatabaseRecords :: Failed to execute statement from batch: {}", error.query());
    });

    // batch insert raw records
    dsl.loadInto(RAW_RECORDS_LB)
      .batchAfter(250)
      .commitAfter(1000)
      .onDuplicateKeyUpdate()
      .onErrorAbort()
      .loadRecords(dbRawRecords)
      .fieldsCorresponding()
      .execute();

    // batch insert parsed records
    recordType.toLoaderOptionsStep(dsl)
      .batchAfter(250)
      .commitAfter(1000)
      .onDuplicateKeyUpdate()
      .onErrorAbort()
      .loadRecords(dbParsedRecords)
      .fieldsCorresponding()
      .execute();

    if (!dbErrorRecords.isEmpty()) {
      // batch insert error records
      dsl.loadInto(ERROR_RECORDS_LB)
        .batchAfter(250)
        .commitAfter(1000)
        .onDuplicateKeyUpdate()
        .onErrorAbort()
        .loadRecords(dbErrorRecords)
        .fieldsCorresponding()
        .execute();
    }
  }

  private List<String> getErrorMessages(List<ErrorRecordsLbRecord> dbErrorRecords) {
    if (CollectionUtils.isEmpty(dbErrorRecords)) {
      return Collections.emptyList();
    }

    return dbErrorRecords.stream()
      .map(errRecord -> format(INVALID_PARSED_RECORD_MESSAGE_TEMPLATE, errRecord.getId(), errRecord.getDescription()))
      .toList();
  }

  private RecordsBatchResponse saveRecords(RecordCollection recordCollection, String snapshotId, RecordType recordType,
                                           String tenantId) {
    Set<UUID> matchedIds = new HashSet<>();
    List<RecordsLbRecord> dbRecords = new ArrayList<>();
    List<RawRecordsLbRecord> dbRawRecords = new ArrayList<>();
    List<Record2<UUID, JSONB>> dbParsedRecords = new ArrayList<>();
    List<ErrorRecordsLbRecord> dbErrorRecords = new ArrayList<>();

    List<String> errorMessages = new ArrayList<>();

    recordCollection.getRecords()
      .stream()
      .map(RecordDaoUtil::ensureRecordHasId)
      .map(RecordDaoUtil::ensureRecordHasMatchedId)
      .map(RecordDaoUtil::ensureRecordHasSuppressDiscovery)
      .map(RecordDaoUtil::ensureRecordForeignKeys)
      .forEach(record -> {
        // collect unique matched ids to query to determine generation
        matchedIds.add(UUID.fromString(record.getMatchedId()));

        // make sure only one snapshot id
        if (!Objects.equals(snapshotId, record.getSnapshotId())) {
          throw new BadRequestException("Batch record collection only supports single snapshot");
        }
        validateRecordType(record, recordType);

        // if record has parsed record, validate by attempting format
        if (Objects.nonNull(record.getParsedRecord())) {
          try {
            recordType.formatRecord(record);
            Record2<UUID, JSONB> dbParsedRecord = recordType.toDatabaseRecord2(record.getParsedRecord());
            dbParsedRecords.add(dbParsedRecord);
          } catch (Exception e) {
            // create error record and remove from record
            Object content = Optional.ofNullable(record.getParsedRecord())
              .map(ParsedRecord::getContent)
              .orElse(null);
            var errorRecord = new ErrorRecord()
              .withId(record.getId())
              .withDescription(e.getMessage())
              .withContent(content);
            errorMessages.add(format(INVALID_PARSED_RECORD_MESSAGE_TEMPLATE, record.getId(), e.getMessage()));
            record.withErrorRecord(errorRecord)
              .withParsedRecord(null)
              .withLeaderRecordStatus(null);
          }
        }

        if (Objects.nonNull(record.getRawRecord())) {
          dbRawRecords.add(RawRecordDaoUtil.toDatabaseRawRecord(record.getRawRecord()));
        }

        if (Objects.nonNull(record.getErrorRecord())) {
          dbErrorRecords.add(ErrorRecordDaoUtil.toDatabaseErrorRecord(record.getErrorRecord()));
        }

        dbRecords.add(RecordDaoUtil.toDatabaseRecord(record));
      });

    try (Connection connection = getConnection(tenantId)) {
      return DSL.using(connection).transactionResult(ctx -> {
        DSLContext dsl = DSL.using(ctx);

        // validate snapshot
        validateSnapshot(UUID.fromString(snapshotId), ctx);

        List<UUID> ids = new ArrayList<>();
        Map<UUID, Integer> matchedGenerations = new HashMap<>();

        // lookup the latest generation by matched id and committed snapshot updated before current snapshot
        dsl.select(RECORDS_LB.MATCHED_ID, RECORDS_LB.ID, RECORDS_LB.GENERATION)
          .distinctOn(RECORDS_LB.MATCHED_ID)
          .from(RECORDS_LB)
          .innerJoin(SNAPSHOTS_LB).on(RECORDS_LB.SNAPSHOT_ID.eq(SNAPSHOTS_LB.ID))
          .where(RECORDS_LB.MATCHED_ID.in(matchedIds)
            .and(SNAPSHOTS_LB.STATUS.in(JobExecutionStatus.COMMITTED, JobExecutionStatus.ERROR, JobExecutionStatus.CANCELLED))
          )
          .orderBy(RECORDS_LB.MATCHED_ID.asc(), RECORDS_LB.GENERATION.desc())
          .fetchStream().forEach(r -> {
            UUID id = r.get(RECORDS_LB.ID);
            UUID matchedId = r.get(RECORDS_LB.MATCHED_ID);
            int generation = r.get(RECORDS_LB.GENERATION);
            ids.add(id);
            matchedGenerations.put(matchedId, generation);
          });

        LOG.info("saveRecords :: recordCollection: {}, dbRecords: {}, matchedGenerations: {}",
          recordCollection.getTotalRecords(), dbRecords.size(), matchedGenerations.size());

        // update matching records state
        if(!ids.isEmpty()) {
          dsl.update(RECORDS_LB)
            .set(RECORDS_LB.STATE, RecordState.OLD)
            .where(RECORDS_LB.ID.in(ids))
            .execute();
        }

        // batch insert records updating generation if required
        var recordsLoadingErrors = dsl.loadInto(RECORDS_LB)
          .batchAfter(1000)
          .bulkAfter(500)
          .commitAfter(1000)
          .onErrorAbort()
          .loadRecords(dbRecords.stream()
            .map(recordDto -> {
              Integer generation = matchedGenerations.get(recordDto.getMatchedId());
              recordDto.setGeneration(Objects.nonNull(generation) ? ++generation : 0);
              LOG.debug("saveRecords:: matchedId: {}, set new generation: {}",
                recordDto.getMatchedId(), generation);
              return recordDto;
            })
            .toList())
          .fieldsCorresponding()
          .execute()
          .errors();

        recordsLoadingErrors.forEach(error -> {
          if (error.exception().sqlState().equals(UNIQUE_VIOLATION_SQL_STATE)) {
            throw new DuplicateEventException("SQL Unique constraint violation prevented repeatedly saving the record");
          }
          LOG.warn("saveRecords:: Error occurred on batch execution: {}", error.exception().getCause().getMessage());
          LOG.debug("saveRecords:: Failed to execute statement from batch: {}", error.query());
        });

        // batch insert raw records
        dsl.loadInto(RAW_RECORDS_LB)
          .batchAfter(250)
          .commitAfter(1000)
          .onDuplicateKeyUpdate()
          .onErrorAbort()
          .loadRecords(dbRawRecords)
          .fieldsCorresponding()
          .execute();

        // batch insert parsed records
        recordType.toLoaderOptionsStep(dsl)
          .batchAfter(250)
          .commitAfter(1000)
          .onDuplicateKeyUpdate()
          .onErrorAbort()
          .loadRecords(dbParsedRecords)
          .fieldsCorresponding()
          .execute();

        if (!dbErrorRecords.isEmpty()) {
          // batch insert error records
          dsl.loadInto(ERROR_RECORDS_LB)
            .batchAfter(250)
            .commitAfter(1000)
            .onDuplicateKeyUpdate()
            .onErrorAbort()
            .loadRecords(dbErrorRecords)
            .fieldsCorresponding()
            .execute();
        }

        return new RecordsBatchResponse()
          .withRecords(recordCollection.getRecords())
          .withTotalRecords(recordCollection.getRecords().size())
          .withErrorMessages(errorMessages);
      });
    } catch (DuplicateEventException e) {
      LOG.info("saveRecords:: Skipped saving records due to duplicate event: {}", e.getMessage());
      throw e;
    } catch (SQLException | DataAccessException ex) {
      LOG.warn("saveRecords:: Failed to save records", ex);
      Throwable throwable = ex.getCause() != null ? ex.getCause() : ex;
      throw new RecordUpdateException(throwable);
    }
  }

  private void validateRecordType(Record recordDto, RecordType recordType) {
    if (recordDto.getRecordType() == null) {
      var error = recordDto.getErrorRecord() != null ? recordDto.getErrorRecord().getDescription() : "";
      throw new BadRequestException(
        StringUtils.defaultIfEmpty(error, String.format("Record with id %s has not record type", recordDto.getId())));
    }

    if (RecordType.valueOf(recordDto.getRecordType().name()) != recordType) {
      throw new BadRequestException("Batch record collection only supports single record type");
    }
  }

  private void validateSnapshot(UUID snapshotId, Configuration ctx) {
    Optional<SnapshotsLbRecord> snapshot = DSL.using(ctx).selectFrom(SNAPSHOTS_LB)
      .where(SNAPSHOTS_LB.ID.eq(snapshotId))
      .fetchOptional();
    if (snapshot.isPresent() && Objects.isNull(snapshot.get().getProcessingStartedDate())) {
      throw new BadRequestException(format(SNAPSHOT_NOT_STARTED_MESSAGE_TEMPLATE, snapshot.get().getStatus()));
    } else if (snapshot.isEmpty()) {
      throw new NotFoundException(format(SNAPSHOT_NOT_FOUND_TEMPLATE, snapshotId));
    }
  }

  @Override
  public Future<Record> updateRecord(Record record, Map<String, String> okapiHeaders) {
    var tenantId = okapiHeaders.get(OKAPI_TENANT_HEADER);
    LOG.trace("updateRecord:: Updating {} record {} for tenant {}", record.getRecordType(), record.getId(), tenantId);
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordById(txQE, record.getId())
        .compose(optionalRecord -> optionalRecord
          .map(r -> insertOrUpdateRecord(txQE, record))
          .orElse(Future.failedFuture(new NotFoundException(format(RECORD_NOT_FOUND_TEMPLATE, record.getId()))))))
      .onSuccess(updated -> recordDomainEventPublisher.publishRecordUpdated(updated, okapiHeaders));
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    Name cte = name(CTE);
    Name prt = name(recordType.getTableName());
    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl -> dsl
      .with(cte.as(dsl.selectCount()
        .from(RECORDS_LB)
        .where(condition.and(recordType.getSourceRecordImplicitCondition()))))
      .select(getRecordFieldsWithCount(prt))
      .from(RECORDS_LB)
      .leftJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
      .rightJoin(dsl.select().from(table(cte))).on(trueCondition())
      .where(condition.and(recordType.getSourceRecordImplicitCondition()))
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit)
    )).map(this::toSourceRecordCollection);
  }

  @Override
  public Flowable<SourceRecord> streamSourceRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    Name prt = name(recordType.getTableName());
    String sql = DSL.select(getRecordFields(prt))
      .from(RECORDS_LB)
      .innerJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
      .where(condition.and(recordType.getSourceRecordImplicitCondition()))
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit)
      .getSQL(ParamType.INLINED);

    return getCachedPool(tenantId)
      .rxGetConnection()
      .flatMapPublisher(conn -> conn.rxBegin()
        .flatMapPublisher(tx -> conn.rxPrepare(sql)
          .flatMapPublisher(pq -> pq.createStream(1)
            .toFlowable()
            .map(this::toRow)
            .map(this::toSourceRecord))
          .doAfterTerminate(tx::commit)
          .doOnError(error -> {
            tx.rollback();
            conn.close();
          })
          .doFinally(conn::close)));
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(List<String> externalIds, IdType idType, RecordType recordType, Boolean deleted, String tenantId) {
    Condition condition = RecordDaoUtil.getExternalIdsCondition(externalIds, idType)
      .and(RecordDaoUtil.filterRecordByDeleted(deleted));
    Name cte = name(CTE);
    Name prt = name(recordType.getTableName());
    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl -> dsl
      .with(cte.as(dsl.selectCount()
        .from(RECORDS_LB)
        .where(condition.and(recordType.getRecordImplicitCondition()))))
      .select(getRecordFieldsWithCount(prt))
      .from(RECORDS_LB)
      .leftJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
      .rightJoin(dsl.select().from(table(cte))).on(trueCondition())
      .where(condition.and(recordType.getSourceRecordImplicitCondition()))
    )).map(this::toSourceRecordCollection);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordByExternalId(String externalId, IdType idType, RecordState state, String tenantId) {
    Condition condition = buildConditionBasedOnState(externalId, idType, state);
    return getSourceRecordByCondition(condition, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordByCondition(Condition condition, String tenantId) {
    return getQueryExecutor(tenantId)
      .transaction(txQE -> txQE.findOneRow(dsl -> dsl.selectFrom(RECORDS_LB)
          .where(condition))
        .map(RecordDaoUtil::toOptionalRecord)
        .compose(optionalRecord -> {
          if (optionalRecord.isPresent()) {
            return lookupAssociatedRecords(txQE, optionalRecord.get(), false)
              .map(RecordDaoUtil::toSourceRecord)
              .map(sourceRecord -> {
                if (Objects.nonNull(sourceRecord.getParsedRecord())) {
                  return Optional.of(sourceRecord);
                }
                return Optional.empty();
              });
          }
          return Future.succeededFuture(Optional.empty());
        }));
  }

  @Override
  public Future<Integer> calculateGeneration(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    return txQE.query(dsl -> dsl.select(max(RECORDS_LB.GENERATION).as(RECORDS_LB.GENERATION))
        .from(RECORDS_LB.innerJoin(SNAPSHOTS_LB).on(RECORDS_LB.SNAPSHOT_ID.eq(SNAPSHOTS_LB.ID)))
        .where(RECORDS_LB.MATCHED_ID.eq(UUID.fromString(record.getMatchedId()))
          .and(SNAPSHOTS_LB.UPDATED_DATE.lessThan(dsl.select(SNAPSHOTS_LB.PROCESSING_STARTED_DATE)
            .from(SNAPSHOTS_LB)
            .where(SNAPSHOTS_LB.ID.eq(UUID.fromString(record.getSnapshotId())))))))
      .map(res -> {
        Integer generation = res.get(RECORDS_LB.GENERATION);
        return Objects.nonNull(generation) ? ++generation : 0;
      });
  }

  @Override
  public Future<ParsedRecord> updateParsedRecord(Record record, Map<String, String> okapiHeaders) {
    var tenantId = okapiHeaders.get(OKAPI_TENANT_HEADER);
    LOG.trace("updateParsedRecord:: Updating {} record {} for tenant {}", record.getRecordType(),
      record.getId(), tenantId);
    return getQueryExecutor(tenantId).transaction(txQE -> GenericCompositeFuture.all(Lists.newArrayList(
        updateExternalIdsForRecord(txQE, record),
        ParsedRecordDaoUtil.update(txQE, record.getParsedRecord(), ParsedRecordDaoUtil.toRecordType(record))
      )).onSuccess(updated -> recordDomainEventPublisher.publishRecordUpdated(record, okapiHeaders))
      .map(res -> record.getParsedRecord()));
  }

  @Override
  public Future<ParsedRecordsBatchResponse> updateParsedRecords(RecordCollection recordCollection, Map<String, String> okapiHeaders) {
    var tenantId = okapiHeaders.get(OKAPI_TENANT_HEADER);
    logRecordCollection("updateParsedRecords:: Updating", recordCollection, tenantId);
    Promise<ParsedRecordsBatchResponse> promise = Promise.promise();
    Context context = Vertx.currentContext();
    if (context == null) return Future.failedFuture("updateParsedRecords must be called by a vertx thread");

    var recordsUpdated = new ArrayList<Record>();
    context.owner().<ParsedRecordsBatchResponse>executeBlocking(blockingPromise ->
      {
        Set<String> recordTypes = new HashSet<>();

        List<Record> records = new ArrayList<>();
        List<String> errorMessages = new ArrayList<>();

        List<UpdateConditionStep<RecordsLbRecord>> recordUpdates = new ArrayList<>();
        List<UpdateConditionStep<org.jooq.Record>> parsedRecordUpdates = new ArrayList<>();

        Field<UUID> prtId = field(name(ID), UUID.class);
        Field<JSONB> prtContent = field(name(CONTENT), JSONB.class);

        List<Record> processedRecords = recordCollection.getRecords()
          .stream()
          .map(this::validateParsedRecordId)
          .peek(record -> {

            // make sure only one record type
            recordTypes.add(record.getRecordType().name());
            if (recordTypes.size() > 1) {
              throw new BadRequestException("Batch record collection only supports single record type");
            }

            UpdateSetFirstStep<RecordsLbRecord> updateFirstStep = DSL.update(RECORDS_LB);
            UpdateSetMoreStep<RecordsLbRecord> updateStep = null;

            // check for external record properties to update
            ExternalIdsHolder externalIdsHolder = record.getExternalIdsHolder();
            AdditionalInfo additionalInfo = record.getAdditionalInfo();
            Metadata metadata = record.getMetadata();

            if (Objects.nonNull(externalIdsHolder)) {
              var recordType = record.getRecordType();
              String externalId = getExternalId(externalIdsHolder, recordType);
              String externalHrid = getExternalHrid(externalIdsHolder, recordType);
              if (StringUtils.isNotEmpty(externalId)) {
                updateStep = updateFirstStep
                  .set(RECORDS_LB.EXTERNAL_ID, UUID.fromString(externalId));
              }
              if (StringUtils.isNotEmpty(externalHrid)) {
                updateStep = (Objects.isNull(updateStep) ? updateFirstStep : updateStep)
                  .set(RECORDS_LB.EXTERNAL_HRID, externalHrid);
              }
            }

            if (Objects.nonNull(additionalInfo)) {
              if (Objects.nonNull(additionalInfo.getSuppressDiscovery())) {
                updateStep = (Objects.isNull(updateStep) ? updateFirstStep : updateStep)
                  .set(RECORDS_LB.SUPPRESS_DISCOVERY, additionalInfo.getSuppressDiscovery());
              }
            }

            if (Objects.nonNull(metadata)) {
              if (StringUtils.isNotEmpty(metadata.getCreatedByUserId())) {
                updateStep = (Objects.isNull(updateStep) ? updateFirstStep : updateStep)
                  .set(RECORDS_LB.CREATED_BY_USER_ID, UUID.fromString(metadata.getCreatedByUserId()));
              }
              if (Objects.nonNull(metadata.getCreatedDate())) {
                updateStep = (Objects.isNull(updateStep) ? updateFirstStep : updateStep)
                  .set(RECORDS_LB.CREATED_DATE, metadata.getCreatedDate().toInstant().atOffset(ZoneOffset.UTC));
              }
              if (StringUtils.isNotEmpty(metadata.getUpdatedByUserId())) {
                updateStep = (Objects.isNull(updateStep) ? updateFirstStep : updateStep)
                  .set(RECORDS_LB.UPDATED_BY_USER_ID, UUID.fromString(metadata.getUpdatedByUserId()));
              }
              if (Objects.nonNull(metadata.getUpdatedDate())) {
                updateStep = (Objects.isNull(updateStep) ? updateFirstStep : updateStep)
                  .set(RECORDS_LB.UPDATED_DATE, metadata.getUpdatedDate().toInstant().atOffset(ZoneOffset.UTC));
              }
            }

            // only attempt update if has id and external values to update
            if (Objects.nonNull(updateStep) && Objects.nonNull(record.getId())) {
              records.add(record);
              recordUpdates.add(updateStep.where(RECORDS_LB.ID.eq(UUID.fromString(record.getId()))));
            }

            try {
              RecordType recordType = toRecordType(record.getRecordType().name());
              recordType.formatRecord(record);

              parsedRecordUpdates.add(
                DSL.update(table(name(recordType.getTableName())))
                  .set(prtContent, JSONB.valueOf(ParsedRecordDaoUtil.normalizeContent(record.getParsedRecord())))
                  .where(prtId.eq(UUID.fromString(record.getParsedRecord().getId())))
              );

            } catch (Exception e) {
              errorMessages.add(format(INVALID_PARSED_RECORD_MESSAGE_TEMPLATE, record.getId(), e.getMessage()));
              // if invalid parsed record, set id to null to filter out
              record.getParsedRecord()
                .setId(null);
            }

          })
          .filter(processedRecord -> Objects.nonNull(processedRecord.getParsedRecord().getId()))
          .toList();

        try (Connection connection = getConnection(tenantId)) {
          DSL.using(connection).transaction(ctx -> {
            DSLContext dsl = DSL.using(ctx);

            // update records
            int[] recordUpdateResults = dsl.batch(recordUpdates).execute();

            // check record update results
            for (int i = 0; i < recordUpdateResults.length; i++) {
              int result = recordUpdateResults[i];
              if (result == 0) {
                errorMessages.add(format("Record with id %s was not updated", records.get(i).getId()));
              }
            }

            // update parsed records
            int[] parsedRecordUpdateResults = dsl.batch(parsedRecordUpdates).execute();

            // check parsed record update results
            for (int i = 0; i < parsedRecordUpdateResults.length; i++) {
              int result = parsedRecordUpdateResults[i];
              var processedRecord = processedRecords.get(i);
              if (result == 0) {
                errorMessages.add(format("Parsed Record with id '%s' was not updated",
                  processedRecord.getParsedRecord().getId()));
              } else {
                recordsUpdated.add(processedRecord);
              }
            }

            blockingPromise.complete(new ParsedRecordsBatchResponse()
              .withErrorMessages(errorMessages)
              .withParsedRecords(recordsUpdated.stream().map(Record::getParsedRecord).toList())
              .withTotalRecords(recordsUpdated.size()));
          });
        } catch (SQLException e) {
          LOG.warn("updateParsedRecords:: Failed to update records", e);
          blockingPromise.fail(e);
        }
      },
      false,
      result -> {
        if (result.failed()) {
          LOG.warn("updateParsedRecords:: Error during update of parsed records", result.cause());
          promise.fail(result.cause());
        } else {
          LOG.debug("updateParsedRecords:: Parsed records update was successful");
          promise.complete(result.result());
        }
      });

    return promise.future()
      .onSuccess(response ->
        recordsUpdated.forEach(updated -> recordDomainEventPublisher.publishRecordUpdated(updated, okapiHeaders))
      );
  }

  @Override
  public Future<Optional<Record>> getRecordByExternalId(String externalId, IdType idType,
                                                        String tenantId) {
    return getQueryExecutor(tenantId)
      .transaction(txQE -> getRecordByExternalId(txQE, externalId, idType));
  }

  @Override
  public Future<Optional<Record>> getRecordByExternalId(ReactiveClassicGenericQueryExecutor txQE,
                                                        String externalId, IdType idType) {
    Condition condition = RecordDaoUtil.getExternalIdCondition(externalId, idType)
      .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL)
        .or(RECORDS_LB.STATE.eq(RecordState.DELETED)));
    return txQE.findOneRow(dsl -> dsl.selectFrom(RECORDS_LB)
        .where(condition)
        .orderBy(RECORDS_LB.GENERATION.sort(SortOrder.DESC))
        .limit(1))
      .map(RecordDaoUtil::toOptionalRecord)
      .compose(optionalRecord -> optionalRecord
        .map(record -> lookupAssociatedRecords(txQE, record, false).map(Optional::of))
        .orElse(Future.failedFuture(new NotFoundException(format(RECORD_NOT_FOUND_BY_ID_TYPE, idType, externalId)))))
      .onFailure(v -> txQE.rollback());
  }

  @Override
  public Future<MarcBibCollection> verifyMarcBibRecords(List<String> marcBibIds, String tenantId) {
    if (marcBibIds.isEmpty()) {
      return Future.succeededFuture(new MarcBibCollection());
    }
    var marcHrid = DSL.field("marc.hrid");

    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl ->
      dsl.selectDistinct(marcHrid)
        .from("(SELECT unnest(" + DSL.array(marcBibIds.toArray()) + ") as hrid) as marc")
        .leftJoin(RECORDS_LB)
        .on(RECORDS_LB.EXTERNAL_HRID.eq(marcHrid.cast(String.class))
          .and(RECORDS_LB.RECORD_TYPE.equal(MARC_BIB)))
        .where(RECORDS_LB.EXTERNAL_HRID.isNull())
    )).map(this::toMarcBibCollection);
  }

  private MarcBibCollection toMarcBibCollection(QueryResult result) {
    MarcBibCollection marcBibCollection = new MarcBibCollection();
    List<String> ids = new ArrayList<>();
    result.stream()
      .map(res -> asRow(res.unwrap()))
      .forEach(row -> ids.add(row.getString(HRID)));
    if (!ids.isEmpty()) {
      marcBibCollection.withInvalidMarcBibIds(ids);
    }
    return marcBibCollection;
  }

  @Override
  public Future<Record> saveUpdatedRecord(ReactiveClassicGenericQueryExecutor txQE, Record newRecord, Record oldRecord, Map<String, String> okapiHeaders) {
    LOG.trace("saveUpdatedRecord:: Saving updated record {}", newRecord.getId());
    return insertOrUpdateRecord(txQE, oldRecord).compose(r -> insertOrUpdateRecord(txQE, newRecord))
      .onSuccess(r -> recordDomainEventPublisher.publishRecordUpdated(r, okapiHeaders));
  }

  @Override
  public Future<Boolean> updateSuppressFromDiscoveryForRecord(String id, IdType idType, Boolean suppress, String tenantId) {
    LOG.trace("updateSuppressFromDiscoveryForRecord:: Updating suppress from discovery with value {} for record with {} {} for tenant {}", suppress, idType, id, tenantId);
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordByExternalId(txQE, id, idType)
        .compose(optionalRecord -> optionalRecord
          .map(record -> RecordDaoUtil.update(txQE, record.withAdditionalInfo(record.getAdditionalInfo().withSuppressDiscovery(suppress))))
          .orElse(Future.failedFuture(new NotFoundException(format(RECORD_NOT_FOUND_BY_ID_TYPE, idType, id))))))
      .map(u -> true);
  }

  @Override
  public Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId) {
    LOG.trace("deleteRecordsBySnapshotId:: Deleting records by snapshotId {} for tenant {}", snapshotId, tenantId);
    return SnapshotDaoUtil.delete(getQueryExecutor(tenantId), snapshotId);
  }

  @Override
  public Future<Boolean> deleteRecordsByExternalId(String externalId, String tenantId) {
    LOG.trace("deleteRecordsByExternalId:: Deleting records by externalId {} for tenant {}", externalId, tenantId);
    var externalUuid = UUID.fromString(externalId);
    return getQueryExecutor(tenantId).transaction(txQE -> txQE
      .execute(dsl -> dsl.deleteFrom(MARC_RECORDS_LB)
        .using(RECORDS_LB)
        .where(MARC_RECORDS_LB.ID.eq(RECORDS_LB.ID))
        .and(RECORDS_LB.EXTERNAL_ID.eq(externalUuid)))
      .compose(u ->
        txQE.execute(dsl -> dsl.deleteFrom(RECORDS_LB)
          .where(RECORDS_LB.EXTERNAL_ID.eq(externalUuid)))
      )).map(u -> true);
  }

  @Override
  public Future<Void> deleteRecords(int lastUpdatedDays, int limit, String tenantId) {
    LOG.trace("deleteRecords:: Deleting record by last {} days for tenant {}", lastUpdatedDays, tenantId);
    Promise<Void> promise = Promise.promise();
    var selectIdsForDeleteQuery = select(RECORDS_LB.ID)
      .from(RECORDS_LB)
      .where(RECORDS_LB.STATE.eq(RecordState.DELETED)).and(RECORDS_LB.UPDATED_DATE.le(OffsetDateTime.now().minusDays(lastUpdatedDays)))
      .orderBy(RECORDS_LB.CREATED_DATE.asc());
    if (limit > 0) {
      selectIdsForDeleteQuery.limit(limit);
    }
    getQueryExecutor(tenantId).execute(dsl -> dsl.deleteFrom(RECORDS_LB).where(RECORDS_LB.ID.in(selectIdsForDeleteQuery)))
      .onSuccess(succeededAr -> promise.complete())
      .onFailure(promise::fail);
    return promise.future();
  }

  /**
   * Deletes old versions of Marc Indexers based on tenant ID.
   *
   * @param tenantId The ID of the tenant for which the Marc Indexers are being deleted.
   * @return A Future of Boolean that completes successfully with a value of 'true' if the deletion was successful,
   * or 'false' if it was not.
   */
  @Override
  public Future<Boolean> deleteMarcIndexersOldVersions(String tenantId) {
    return executeInTransaction(txQE -> acquireLock(txQE, INDEXERS_DELETION_LOCK_NAMESPACE_ID, tenantId.hashCode())
      .compose(isLockAcquired -> {
        if (Boolean.FALSE.equals(isLockAcquired)) {
          LOG.info("deleteMarcIndexersOldVersions:: Previous marc_indexers old version deletion still ongoing, tenantId: '{}'", tenantId);
          return Future.succeededFuture(false);
        }
        return deleteMarcIndexersOldVersions(txQE, tenantId);
      }), tenantId);
  }

  private Future<Boolean> deleteMarcIndexersOldVersions(ReactiveClassicGenericQueryExecutor txQE, String tenantId) {
    LOG.trace("deleteMarcIndexersOldVersions:: Deleting old marc indexers versions tenantId={}", tenantId);
    long startTime = System.nanoTime();


    return txQE.execute(dsl ->
        dsl.createTemporaryTableIfNotExists(DELETE_MARC_INDEXERS_TEMP_TABLE)
          .column(MARC_ID, SQLDataType.UUID)
          .constraint(primaryKey(MARC_ID))
          .onCommitDrop()
      )
      .compose(ar -> txQE.execute(
          dsl -> dsl.query(CALL_DELETE_OLD_MARC_INDEXERS_VERSIONS_PROCEDURE))
        .onFailure(th -> LOG.error("Something happened while deleting old marc_indexers versions tenantId={}", tenantId, th))
      )
      .compose(res -> {
        Table<Record1<UUID>> subquery = select(field(MARC_ID, SQLDataType.UUID))
          .from(table(DELETE_MARC_INDEXERS_TEMP_TABLE)).asTable("subquery");

        return txQE.execute(dsl ->
            dsl.update(table(OLD_RECORDS_TRACKING_TABLE))
              .set(field(HAS_BEEN_PROCESSED_FLAG), true)
              .where(field(MARC_ID_COLUMN).in(select(subquery.field(MARC_ID, SQLDataType.UUID)).from(subquery)))
          )
          .compose(updateResult ->
            txQE.execute(dsl ->
              dsl.update(MARC_RECORDS_TRACKING)
                .set(MARC_RECORDS_TRACKING.IS_DIRTY, false)
                .where(MARC_RECORDS_TRACKING.MARC_ID
                  .in(select(subquery.field(MARC_ID, SQLDataType.UUID)).from(subquery)))
            )
          )
          .onFailure(th -> {
            double durationSeconds = TenantUtil.calculateDurationSeconds(startTime);
            LOG.error("Something happened while updating tracking tables tenantId={}. Duration= {}s", tenantId, durationSeconds, th);
          });
      })
      .map(res -> {
        double durationSeconds = TenantUtil.calculateDurationSeconds(startTime);
        LOG.info("deleteMarcIndexersOldVersions:: Completed successfully for tenantId={}. Duration= {}s", tenantId, durationSeconds);
        return true;
      })
      .recover(th -> {
        double durationSeconds = TenantUtil.calculateDurationSeconds(startTime);
        LOG.error("deleteMarcIndexersOldVersions:: Failed for tenantId={}. Duration= {}s", tenantId, durationSeconds, th);
        return Future.succeededFuture(false);
      });
  }

  @Override
  public Future<Void> updateRecordsState(String matchedId, RecordState state, RecordType recordType, String tenantId) {
    LOG.trace("updateRecordsState:: Updating records state with value {} by matchedId {} for tenant {}", state, matchedId, tenantId);
    if (recordType == RecordType.MARC_AUTHORITY && state == RecordState.DELETED) {
      return updateMarcAuthorityRecordsStateAsDeleted(matchedId, tenantId);
    }

    Promise<Void> promise = Promise.promise();
    getQueryExecutor(tenantId).execute(dsl -> dsl.update(RECORDS_LB)
        .set(RECORDS_LB.STATE, state)
        .where(RECORDS_LB.MATCHED_ID.eq(UUID.fromString(matchedId)))
      )
      .onSuccess(succeededAr -> promise.complete())
      .onFailure(promise::fail);
    return promise.future();
  }

  public Future<Void> updateMarcAuthorityRecordsStateAsDeleted(String matchedId, String tenantId) {
    Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(matchedId));
    return getQueryExecutor(tenantId).transaction(txQE -> getRecords(condition, RecordType.MARC_AUTHORITY, new ArrayList<>(), 0, RECORDS_LIMIT, tenantId)
      .compose(recordCollection -> {
        List<Future<Record>> futures = recordCollection.getRecords().stream()
          .map(recordToUpdate -> updateMarcAuthorityRecordWithDeletedState(txQE, ensureRecordForeignKeys(recordToUpdate)))
          .toList();

        Promise<Void> result = Promise.promise();
        GenericCompositeFuture.all(futures).onComplete(ar -> {
          if (ar.succeeded()) {
            result.complete();
          } else {
            result.fail(ar.cause());
            LOG.warn("Error during update records state to DELETED", ar.cause());
          }
        });
        return result.future();
      }));
  }

  private Future<Record> updateMarcAuthorityRecordWithDeletedState(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    record.withState(Record.State.DELETED);
    if (Objects.nonNull(record.getParsedRecord())) {
      record.getParsedRecord().setId(record.getId());
      ParsedRecordDaoUtil.updateLeaderStatus(record.getParsedRecord(), 'd');
      return ParsedRecordDaoUtil.update(txQE, record.getParsedRecord(), ParsedRecordDaoUtil.toRecordType(record))
        .compose(parsedRecord -> {
          record.withLeaderRecordStatus(ParsedRecordDaoUtil.getLeaderStatus(record.getParsedRecord()));
          return RecordDaoUtil.update(txQE, record);
        });
    }
    return RecordDaoUtil.update(txQE, record);

  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

  private PgPool getCachedPool(String tenantId) {
    return postgresClientFactory.getCachedPool(tenantId);
  }

  private Connection getConnection(String tenantId) throws SQLException {
    return postgresClientFactory.getConnection(tenantId);
  }

  private Row toRow(io.vertx.reactivex.sqlclient.Row row) {
    return row.getDelegate();
  }

  private Row asRow(Row row) {
    return row;
  }

  private Future<Optional<Record>> lookupAssociatedRecords(ReactiveClassicGenericQueryExecutor txQE, Optional<Record> record, boolean includeErrorRecord) {
    if (record.isPresent()) {
      return lookupAssociatedRecords(txQE, record.get(), includeErrorRecord).map(Optional::of);
    }
    return Future.succeededFuture(record);
  }

  private Future<Record> lookupAssociatedRecords(ReactiveClassicGenericQueryExecutor txQE, Record record, boolean includeErrorRecord) {
    List<Future<Record>> futures = new ArrayList<>();
    futures.add(RawRecordDaoUtil.findById(txQE, record.getId()).map(rr -> {
      rr.ifPresent(record::withRawRecord);
      return record;
    }));
    futures.add(ParsedRecordDaoUtil.findById(txQE, record.getId(), ParsedRecordDaoUtil.toRecordType(record)).map(pr -> {
      pr.ifPresent(record::withParsedRecord);
      return record;
    }));
    if (includeErrorRecord) {
      futures.add(ErrorRecordDaoUtil.findById(txQE, record.getId()).map(er -> {
        er.ifPresent(record::withErrorRecord);
        return record;
      }));
    }
    return GenericCompositeFuture.all(futures).map(res -> record);
  }

  private Future<Record> insertOrUpdateRecord(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    return RawRecordDaoUtil.save(txQE, record.getRawRecord())
      .compose(rawRecord -> {
        if (Objects.nonNull(record.getParsedRecord())) {
          return insertOrUpdateParsedRecord(txQE, record);
        }
        return Future.succeededFuture(null);
      })
      .compose(parsedRecord -> {
        if (Objects.nonNull(record.getErrorRecord())) {
          return ErrorRecordDaoUtil.save(txQE, record.getErrorRecord());
        }
        return Future.succeededFuture(null);
      })
      .compose(errorRecord -> RecordDaoUtil.save(txQE, record)).map(savedRecord -> {
        if (Objects.nonNull(record.getRawRecord())) {
          savedRecord.withRawRecord(record.getRawRecord());
        }
        if (Objects.nonNull(record.getParsedRecord())) {
          savedRecord.withParsedRecord(record.getParsedRecord());
        }
        if (Objects.nonNull(record.getErrorRecord())) {
          savedRecord.withErrorRecord(record.getErrorRecord());
        }
        return savedRecord;
      });
  }

  private Future<ParsedRecord> insertOrUpdateParsedRecord(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    try {
      LOG.trace("insertOrUpdateParsedRecord:: Inserting or updating {} parsed record", record.getRecordType());
      // attempt to format record to validate
      RecordType recordType = toRecordType(record.getRecordType().name());
      recordType.formatRecord(record);
      return ParsedRecordDaoUtil.save(txQE, record.getParsedRecord(), ParsedRecordDaoUtil.toRecordType(record))
        .map(parsedRecord -> {
          record.withLeaderRecordStatus(ParsedRecordDaoUtil.getLeaderStatus(record.getParsedRecord()));
          return parsedRecord;
        });
    } catch (Exception e) {
      LOG.warn("insertOrUpdateParsedRecord:: Couldn't format {} record", record.getRecordType(), e);
      record.withErrorRecord(new ErrorRecord()
        .withId(record.getId())
        .withDescription(e.getMessage())
        .withContent(record.getParsedRecord().getContent()));
      record.withParsedRecord(null)
        .withLeaderRecordStatus(null);
      return Future.succeededFuture(null);
    }
  }

  private Future<Boolean> updateExternalIdsForRecord(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    LOG.trace("updateExternalIdsForRecord:: Updating external ids for {} record", record.getRecordType());
    return RecordDaoUtil.findById(txQE, record.getId())
      .map(optionalRecord -> {
        if (optionalRecord.isPresent()) {
          return optionalRecord;
        }
        String rollBackMessage = format(RECORD_NOT_FOUND_TEMPLATE, record.getId());
        throw new NotFoundException(rollBackMessage);
      })
      .map(Optional::get)
      .compose(persistedRecord -> {
        persistedRecord.withExternalIdsHolder(record.getExternalIdsHolder())
          .withAdditionalInfo(record.getAdditionalInfo())
          .withMetadata(record.getMetadata());
        return RecordDaoUtil.update(txQE, persistedRecord)
          .map(update -> true);
      });
  }

  private Record validateParsedRecordId(Record recordDto) {
    if (Objects.isNull(recordDto.getParsedRecord()) || StringUtils.isEmpty(recordDto.getParsedRecord().getId())) {
      throw new BadRequestException("Each parsed record should contain an id");
    }
    return recordDto;
  }

  private Field<?>[] getRecordFields(Name prt) {
    return (Field<?>[]) ArrayUtils.addAll(RECORD_FIELDS, new Field<?>[]{
      field(TABLE_FIELD_TEMPLATE, JSONB.class, prt, name(CONTENT))
    });
  }

  private Field<?>[] getRecordFieldsWithCount(Name prt) {
    return (Field<?>[]) ArrayUtils.addAll(getRecordFields(prt), new Field<?>[]{
      COUNT_FIELD
    });
  }

  private Field<?>[] getAllRecordFields(Name prt) {
    return (Field<?>[]) ArrayUtils.addAll(RECORD_FIELDS, new Field<?>[]{
      field(TABLE_FIELD_TEMPLATE, JSONB.class, prt, name(CONTENT)).as(PARSED_RECORD_CONTENT),
      RAW_RECORDS_LB.CONTENT.as(RAW_RECORD_CONTENT),
      ERROR_RECORDS_LB.CONTENT.as(ERROR_RECORD_CONTENT),
      ERROR_RECORDS_LB.DESCRIPTION
    });
  }

  private Field<?>[] getAllRecordFieldsWithCount(Name prt) {
    return (Field<?>[]) ArrayUtils.addAll(getAllRecordFields(prt), new Field<?>[]{
      COUNT_FIELD
    });
  }

  private Field<?>[] getStrippedParsedRecordWithCount(Name prt) {
    return new Field<?>[]{COUNT_FIELD,
      RECORDS_LB.ID, RECORDS_LB.EXTERNAL_ID,
      RECORDS_LB.STATE, RECORDS_LB.RECORD_TYPE,
      field(TABLE_FIELD_TEMPLATE, JSONB.class, prt, name(CONTENT)).as(PARSED_RECORD_CONTENT)
    };
  }

  private RecordCollection toRecordCollection(QueryResult result) {
    RecordCollection recordCollection = new RecordCollection().withTotalRecords(0);
    List<Record> records = result.stream().map(res -> asRow(res.unwrap())).map(row -> {
      recordCollection.setTotalRecords(row.getInteger(COUNT));
      return toRecord(row);
    })
      .toList();
    if (!records.isEmpty() && Objects.nonNull(records.get(0).getId())) {
      recordCollection.withRecords(records);
    }
    return recordCollection;
  }

  private StrippedParsedRecordCollection toStrippedParsedRecordCollection(QueryResult result) {
    StrippedParsedRecordCollection recordCollection = new StrippedParsedRecordCollection().withTotalRecords(0);
    List<StrippedParsedRecord> records = result.stream().map(res -> asRow(res.unwrap())).map(row -> {
      recordCollection.setTotalRecords(row.getInteger(COUNT));
      return toStrippedParsedRecord(row);
    })
      .toList();
    if (!records.isEmpty() && Objects.nonNull(records.get(0).getId())) {
      recordCollection.withRecords(records);
    }
    return recordCollection;
  }

  /*
   * Code to avoid the occurrence of records when limit equals to zero
   */
  private RecordCollection toRecordCollectionWithLimitCheck(QueryResult result, int limit) {
    // Validation to ignore records insertion to the returned recordCollection when limit equals zero
    if (limit == 0) {
      return new RecordCollection().withTotalRecords(asRow(result.unwrap()).getInteger(COUNT));
    } else {
      return toRecordCollection(result);
    }
  }

  private SourceRecordCollection toSourceRecordCollection(QueryResult result) {
    SourceRecordCollection sourceRecordCollection = new SourceRecordCollection().withTotalRecords(0);
    List<SourceRecord> sourceRecords = result.stream().map(res -> asRow(res.unwrap())).map(row -> {
      sourceRecordCollection.setTotalRecords(row.getInteger(COUNT));
      return RecordDaoUtil.toSourceRecord(RecordDaoUtil.toRecord(row))
        .withParsedRecord(ParsedRecordDaoUtil.toParsedRecord(row));
    })
      .toList();
    if (!sourceRecords.isEmpty() && Objects.nonNull(sourceRecords.get(0).getRecordId())) {
      sourceRecordCollection.withSourceRecords(sourceRecords);
    }
    return sourceRecordCollection;
  }

  private SourceRecord toSourceRecord(Row row) {
    SourceRecord sourceRecord = RecordDaoUtil.toSourceRecord(row);
    ParsedRecord parsedRecord = ParsedRecordDaoUtil.toParsedRecord(row);
    if (Objects.nonNull(parsedRecord.getContent())) {
      sourceRecord.setParsedRecord(parsedRecord);
    }
    return sourceRecord;
  }

  private Record toRecord(Row row) {
    Record recordDto = RecordDaoUtil.toRecord(row);
    RawRecord rawRecord = RawRecordDaoUtil.toJoinedRawRecord(row);
    if (Objects.nonNull(rawRecord.getContent())) {
      recordDto.setRawRecord(rawRecord);
    }
    ParsedRecord parsedRecord = ParsedRecordDaoUtil.toJoinedParsedRecord(row);
    if (Objects.nonNull(parsedRecord.getContent())) {
      recordDto.setParsedRecord(parsedRecord);
    }
    ErrorRecord errorRecord = ErrorRecordDaoUtil.toJoinedErrorRecord(row);
    if (Objects.nonNull(errorRecord.getContent())) {
      recordDto.setErrorRecord(errorRecord);
    }
    return recordDto;
  }

  private Record toRecord(org.jooq.Record dbRecord) {
    Record recordDto = RecordDaoUtil.toRecord(dbRecord);
    RawRecord rawRecord = RawRecordDaoUtil.toJoinedRawRecord(dbRecord);
    if (Objects.nonNull(rawRecord.getContent())) {
      recordDto.setRawRecord(rawRecord);
    }

    ParsedRecord parsedRecord = ParsedRecordDaoUtil.toJoinedParsedRecord(dbRecord);
    if (Objects.nonNull(parsedRecord.getContent())) {
      recordDto.setParsedRecord(parsedRecord);
    }
    ErrorRecord errorRecord = ErrorRecordDaoUtil.toJoinedErrorRecord(dbRecord);
    if (Objects.nonNull(errorRecord.getContent())) {
      recordDto.setErrorRecord(errorRecord);
    }
    return recordDto;
  }

  private StrippedParsedRecord toStrippedParsedRecord(Row row) {
    StrippedParsedRecord strippedRecord = RecordDaoUtil.toStrippedParsedRecord(row);
    ParsedRecord parsedRecord = ParsedRecordDaoUtil.toJoinedParsedRecord(row);
    if (Objects.nonNull(parsedRecord.getContent())) {
      strippedRecord.setParsedRecord(parsedRecord);
    }
    return strippedRecord;
  }

  private void logRecordCollection(String msg, RecordCollection recordCollection, String tenantId) {
    if (LOG.isTraceEnabled()) {
      recordCollection.getRecords().forEach(e -> LOG.trace("{} {} record {} for tenant {}", msg, e.getRecordType(), e.getId(), tenantId));
    }
  }

  private static Condition buildConditionBasedOnState(String externalId, IdType idType, RecordState state) {
    Condition condition;
    if (state == RecordState.ACTUAL) {
      condition = RecordDaoUtil.getExternalIdCondition(externalId, idType)
        .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL)
          .or(RECORDS_LB.STATE.eq(RecordState.DELETED)))
        .and(RECORDS_LB.LEADER_RECORD_STATUS.isNotNull());
    } else {
      condition = RecordDaoUtil.getExternalIdCondition(externalId, idType)
        .and(RECORDS_LB.STATE.eq(state))
        .and(RECORDS_LB.LEADER_RECORD_STATUS.isNotNull());
    }
    return condition;
  }

}
