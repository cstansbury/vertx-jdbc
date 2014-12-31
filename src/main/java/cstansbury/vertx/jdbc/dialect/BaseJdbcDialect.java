package cstansbury.vertx.jdbc.dialect;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import cstansbury.vertx.jdbc.JdbcDialect;

/**
 * 
 * @author cstansbury
 */
public class BaseJdbcDialect implements JdbcDialect {

  // -------------------------------------------------------------------------
  // Static Variables
  // -------------------------------------------------------------------------

  protected static final JsonArray EMPTY_JSON_ARRAY = new JsonArray();

  // -------------------------------------------------------------------------
  // Member Variables
  // -------------------------------------------------------------------------

  private boolean mSupportsParameterMetaData = true;
  
  private String mDateFormat = "yyyy-MM-dd";
  
  private String mTimestampFormat = "yyyy-MM-dd HH:mm:ss";
  
  // -------------------------------------------------------------------------
  // JdbcDialect Protocol
  // -------------------------------------------------------------------------

  @Override
  public Object executeCall(final Connection connection, final JsonObject requestBody) throws SQLException {
    return null;
  }
  
  @Override
  public Object executeQuery(final Connection connection, final JsonObject requestBody) throws SQLException {
    Object responseBody = null;
    
    try (final PreparedStatement statement = prepareQueryStatement(connection, requestBody)) {
      final List<JsonArray> allBindParams = getAllBindParams(requestBody, statement);
      final JsonArray responseRows = new JsonArray();
      
      for (final JsonArray bindParams : allBindParams) {
        try (final ResultSet resultSet = applyBindParams(statement, bindParams).executeQuery()) {
          responseRows.add(parseResultSetArray(resultSet));
        }
      }
      
      if (responseRows.size() == 1) {
        responseBody = responseRows.getJsonArray(0);
      } else {
        responseBody = responseRows;
      }
    }
    
    return responseBody;
  }

  @Override
  public Object executeUpdate(final Connection connection, final JsonObject requestBody) throws SQLException {
    Boolean originalAutoCommit = null;
    Object responseBody = null;
    
    try (final PreparedStatement statement = prepareUpdateStatement(connection, requestBody)) {
      final List<JsonArray> allBindParams = getAllBindParams(requestBody, statement);
      final JsonArray responseRows = new JsonArray();
      
      if (allBindParams.size() > 1) {
        originalAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
      }
      
      for (final JsonArray bindParams : allBindParams) {
        final JsonObject updateResult = new JsonObject();
        final int rowCount = applyBindParams(statement, bindParams).executeUpdate();

        updateResult.put("rowCount", rowCount);
        try (final ResultSet resultSet = statement.getGeneratedKeys()) {
          final JsonArray generatedKeys = parseResultSetArray(resultSet);
          if (generatedKeys.size() > 0) {
            updateResult.put("generatedKeys", generatedKeys);
          }
        }
        
        responseRows.add(updateResult);
      }
      
      if (responseRows.size() == 1) {
        responseBody = responseRows.getValue(0);
      } else {
        responseBody = responseRows;
      }
    } finally {
      if (originalAutoCommit != null) {
        try {
          if (responseBody != null) {
            connection.commit();
          } else {
            connection.rollback();
          }
        } finally {
          connection.setAutoCommit(originalAutoCommit);
        }
      } 
    }

    return responseBody;
  }
  
  // -------------------------------------------------------------------------
  // Protected Bind Param Protocol
  // -------------------------------------------------------------------------

  protected PreparedStatement applyBindParams(final PreparedStatement statement, final JsonArray bindParams) throws SQLException {
    for (int i = 0; i < bindParams.size(); i++) {
      final Object value = bindParams.getValue(i);
      final int parameterIndex = i + 1;
      if (value != null) {
        statement.setObject(parameterIndex, value);
      } else if (mSupportsParameterMetaData){
        statement.setNull(parameterIndex, statement.getParameterMetaData().getParameterType(parameterIndex));
      } else {
        statement.setNull(parameterIndex, Types.VARCHAR);
      }
    }
    
    return statement;
  }
  
  protected List<JsonArray> getAllBindParams(final JsonObject requestBody, final PreparedStatement preparedStatement) {
    final JsonArray requestParams = requestBody.getJsonArray("bind");
    final int requestParamsSize = requestParams == null ? 0 : requestParams.size();
    final List<JsonArray> bindParamsList = new ArrayList<>(Math.max(1, requestParamsSize));
    
    if (requestParamsSize == 0) {
      bindParamsList.add(EMPTY_JSON_ARRAY);
    } else {
      final Iterator<Object> iterator = requestParams.iterator();
      Object value = iterator.next();
      if (value instanceof JsonArray) {
        do {
          bindParamsList.add((JsonArray)value);
          value = iterator.hasNext() ? iterator.next() : null;
        } while (value != null);
      } else {
        bindParamsList.add(requestParams);
      }
    }
    
    return bindParamsList;
  }

  // -------------------------------------------------------------------------
  // Protected ResultSet Protocol
  // -------------------------------------------------------------------------

  protected JsonArray parseResultSetArray(final ResultSet resultSet) throws SQLException {
    final JsonArray jsonArray = new JsonArray();
    
    while (resultSet.next()) {
      jsonArray.add(parseResultSetObject(resultSet));
    }

    return jsonArray;
  }

  public JsonObject parseResultSetObject(final ResultSet resultSet) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    final JsonObject resultRow = new JsonObject();
    
    for (int i = 0; i < columnCount; i++) {
      final int columnIndex = i + 1;
      final String columnName = metaData.getColumnName(columnIndex).toLowerCase();
      switch (metaData.getColumnType(columnIndex)) {
      case Types.DATE:
        final Date date = resultSet.getDate(columnIndex);
        resultRow.put(columnName, date == null ? null : new SimpleDateFormat(mDateFormat).format(date));
        break;
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        final Date timestamp = resultSet.getTimestamp(columnIndex);
        resultRow.put(columnName, timestamp == null ? null : new SimpleDateFormat(mTimestampFormat).format(timestamp));
        break;
      default:
        resultRow.put(columnName, resultSet.getObject(columnIndex));
      }
    }
    
    return resultRow;
  }

  // -------------------------------------------------------------------------
  // Protected PreparedStatement Protocol
  // -------------------------------------------------------------------------

  protected PreparedStatement prepareQueryStatement(final Connection connection, final JsonObject requestBody) throws SQLException {
    final String sql = requestBody.getString("sql");
    PreparedStatement statement = null;
    
    if (sql == null) {
      // todo throw exception
    }
    
    statement = connection.prepareStatement(sql);
    
    return statement;
  }
  
  protected PreparedStatement prepareUpdateStatement(final Connection connection, final JsonObject requestBody) throws SQLException {
    final String sql = requestBody.getString("sql");
    PreparedStatement statement = null;
    
    if (sql == null) {
      // todo throw exception
    }
    
    // TODO lookup in statement cache?

    final Boolean generatedKeys = requestBody.getBoolean("generatedKeys");
    final JsonArray generatedKeyIndices = requestBody.getJsonArray("generatedKeyIndices");

    if (Boolean.TRUE.equals(generatedKeys)) {
      statement = connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
    } else if (generatedKeyIndices != null) {
      final int[] columnIndexes = new int[generatedKeyIndices.size()];
      for (int i = 0; i < columnIndexes.length; i++) {
        columnIndexes[i] = generatedKeyIndices.getInteger(i);
      }
      statement = connection.prepareStatement(sql, columnIndexes);
    } else {
      statement = connection.prepareStatement(sql);
    }
    
    return statement;
  }

  // -------------------------------------------------------------------------
  // Member Accessors
  // -------------------------------------------------------------------------

  public void setDateFormat(final String dateFormat) {
    mDateFormat = dateFormat;
  }
  
  public void setTimestampFormat(final String timestampFormat) {
    mTimestampFormat = timestampFormat;
  }
  
  public void setSupportsParameterMetaData(final boolean supportsParameterMetaData) {
    mSupportsParameterMetaData = supportsParameterMetaData;
  }
  
}
