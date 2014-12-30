package cstansbury.vertx.jdbc.action;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import cstansbury.vertx.jdbc.JdbcDialect;
import cstansbury.vertx.jdbc.JdbcUtils;

public class JdbcUpdateAction extends BaseJdbcAction {

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------

  public JdbcUpdateAction(final JdbcDialect dialect) {
    super(dialect);
  }

  // -------------------------------------------------------------------------
  // Overridden JdbcAction Protocol
  // -------------------------------------------------------------------------

  public Object execute(final Connection connection, final JsonObject requestBody) throws SQLException {
    final PreparedStatement statement = prepareStatement(connection, requestBody);
    Object responseBody = null;
    
    try {
      final List<JsonArray> allBindParams = getAllBindParams(requestBody, statement);
      final JsonArray responseRows = new JsonArray();
      
      for (final JsonArray bindParams : allBindParams) {
        final JsonObject updateResult = new JsonObject();
        final int rowCount = applyBindParams(statement, bindParams).executeUpdate();
        final ResultSet resultSet = statement.getGeneratedKeys();
        
        updateResult.put("rowCount", rowCount);
        
        try {
          if (resultSet.next()) {
            final JsonArray generatedKeys = new JsonArray();
            do {
              generatedKeys.add(parseResultSet(resultSet));
            } while (resultSet.next());
            updateResult.put("generatedKeys", generatedKeys);
          }
        } finally {
          JdbcUtils.closeQuietly(resultSet);
        }
        
        responseRows.add(updateResult);
      }
      
      if (responseRows.size() == 1) {
        responseBody = responseRows.getValue(0);
      } else {
        responseBody = responseRows;
      }
    } finally {
      JdbcUtils.closeQuietly(statement);
    }

    return responseBody;
  }

  // -------------------------------------------------------------------------
  // Protected Protocol
  // -------------------------------------------------------------------------

  protected PreparedStatement prepareStatement(final Connection connection, final JsonObject requestBody) throws SQLException {
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

  

}
