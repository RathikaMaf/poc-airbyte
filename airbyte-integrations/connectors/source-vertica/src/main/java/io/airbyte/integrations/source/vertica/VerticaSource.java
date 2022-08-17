/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.vertica;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.factory.DatabaseDriver;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.db.jdbc.streaming.AdaptiveStreamingQueryConfig;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource;
import io.airbyte.integrations.source.jdbc.dto.JdbcPrivilegeDto;
import io.airbyte.integrations.source.relationaldb.TableInfo;
import io.airbyte.protocol.models.CommonField;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VerticaSource extends AbstractJdbcSource<JDBCType> implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(VerticaSource.class);
  public static final String DRIVER_CLASS = DatabaseDriver.VERTICA.getDriverClassName();
  private List<String> schemas;

  // todo (cgardens) - clean up passing the dialect as null versus explicitly adding the case to the
  // constructor.
  public VerticaSource() {
    super(DRIVER_CLASS, AdaptiveStreamingQueryConfig::new, JdbcUtils.getDefaultSourceOperations());
  }

  @Override
  public JsonNode toDatabaseConfig(final JsonNode verticaConfig) {
    final List<String> additionalProperties = new ArrayList<>();
    final ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder()
        .put(JdbcUtils.USERNAME_KEY, verticaConfig.get(JdbcUtils.USERNAME_KEY).asText())
        .put(JdbcUtils.PASSWORD_KEY, verticaConfig.get(JdbcUtils.PASSWORD_KEY).asText())
        .put(JdbcUtils.JDBC_URL_KEY, String.format(DatabaseDriver.VERTICA.getUrlFormatString(),
        verticaConfig.get(JdbcUtils.HOST_KEY).asText(),
        verticaConfig.get(JdbcUtils.PORT_KEY).asInt(),
        verticaConfig.get(JdbcUtils.DATABASE_KEY).asText()));

    if (verticaConfig.has(JdbcUtils.SCHEMAS_KEY) && verticaConfig.get(JdbcUtils.SCHEMAS_KEY).isArray()) {
      schemas = new ArrayList<>();
      for (final JsonNode schema : verticaConfig.get(JdbcUtils.SCHEMAS_KEY)) {
        schemas.add(schema.asText());
      }

      if (schemas != null && !schemas.isEmpty()) {
        additionalProperties.add("currentSchema=" + String.join(",", schemas));
      }
    }

    //addSsl(additionalProperties);

    //builder.put(JdbcUtils.CONNECTION_PROPERTIES_KEY, String.join("&", additionalProperties));
    

    return Jsons.jsonNode(builder
        .build());
  }

  // private void addSsl(final List<String> additionalProperties) {
  //   additionalProperties.add("ssl=true");
  //   additionalProperties.add("sslfactory=com.amazon.redshift.ssl.NonValidatingFactory");
  // }

  @Override
  public List<TableInfo<CommonField<JDBCType>>> discoverInternal(final JdbcDatabase database) throws Exception {
    if (schemas != null && !schemas.isEmpty()) {
      // process explicitly selected (from UI) schemas
      final List<TableInfo<CommonField<JDBCType>>> internals = new ArrayList<>();
      for (final String schema : schemas) {
        LOGGER.debug("Discovering schema: {}", schema);
        internals.addAll(super.discoverInternal(database, schema));
      }
      for (final TableInfo<CommonField<JDBCType>> info : internals) {
        LOGGER.debug("Found table (schema: {}): {}", info.getNameSpace(), info.getName());
      }
      return internals;
    } else {
      LOGGER.info("No schemas explicitly set on UI to process, so will process all of existing schemas in DB");
      return super.discoverInternal(database);
    }
  }

  @Override
  public Set<String> getExcludedInternalNameSpaces() {
    //return Set.of("information_schema", "pg_catalog", "pg_internal", "catalog_history");
    return Set.of("v_monitor", "v_catalog", "online_sales", "store");
  }

  @Override
  public Set<JdbcPrivilegeDto> getPrivilegesTableForCurrentUser(final JdbcDatabase database, final String schema) throws SQLException {    
    return new HashSet<>(database.bufferedResultSetQuery(
        connection -> {
          connection.setAutoCommit(true);
          final PreparedStatement ps = connection.prepareStatement(              
              "SELECT schema_name, table_name "
                  + "FROM   all_tables "
                  + "WHERE schema_name = ?;");
                  
          ps.setString(1, schema);
          return ps.executeQuery();
        },
        resultSet -> {
          final JsonNode json = sourceOperations.rowToJson(resultSet);          
          return JdbcPrivilegeDto.builder()
              .schemaName(json.get("schema_name").asText())
              .tableName(json.get("table_name").asText())
              .build();
        }));
  }

  public static void main(final String[] args) throws Exception {
    final Source source = new VerticaSource();
    LOGGER.info("starting source: {}", VerticaSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("completed source: {}", VerticaSource.class);
  }

}
