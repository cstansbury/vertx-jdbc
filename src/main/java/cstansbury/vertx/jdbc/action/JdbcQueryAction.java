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

public class JdbcQueryAction extends BaseJdbcAction {

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------

  public JdbcQueryAction(final JdbcDialect dialect) {
    super(dialect);
  }

  // -------------------------------------------------------------------------
  // Overridden JdbcAction Protocol
  // -------------------------------------------------------------------------

  @Override
  public Object execute(final Connection connection, final JsonObject requestBody) throws SQLException {
    final PreparedStatement statement = prepareStatement(connection, requestBody);
    Object responseBody = null;
    
    try {
      final List<JsonArray> allBindParams = getAllBindParams(requestBody, statement);
      final JsonArray responseRows = new JsonArray();
      
      for (final JsonArray bindParams : allBindParams) {
        final JsonArray bindRows = new JsonArray();
        final ResultSet resultSet = applyBindParams(statement, bindParams).executeQuery();
        try {
          while (resultSet.next()) {
            bindRows.add(parseResultSet(resultSet));
          };
          responseRows.add(bindRows);
        } finally {
          JdbcUtils.closeQuietly(resultSet);
        }
      }
      
      if (responseRows.size() == 1) {
        responseBody = responseRows.getJsonArray(0);
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
    
    statement = connection.prepareStatement(sql);
    
    return statement;
  }

}
