package cstansbury.vertx.jdbc.action;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cstansbury.vertx.jdbc.JdbcAction;
import cstansbury.vertx.jdbc.JdbcDialect;

public abstract class BaseJdbcAction implements JdbcAction {

  // -------------------------------------------------------------------------
  // Static Variables
  // -------------------------------------------------------------------------

  protected static final JsonArray EMPTY_JSON_ARRAY = new JsonArray();
  
  // -------------------------------------------------------------------------
  // Member Variables
  // -------------------------------------------------------------------------

  private final JdbcDialect mDialect;
  
  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------

  public BaseJdbcAction(final JdbcDialect dialect) {
    mDialect = dialect;
  }
  
  // -------------------------------------------------------------------------
  // Protected Protocol
  // -------------------------------------------------------------------------

  protected PreparedStatement applyBindParams(final PreparedStatement statement, final JsonArray bindParams) throws SQLException {
    mDialect.applyBindParams(statement, bindParams);
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
  
  protected JsonObject parseResultSet(final ResultSet resultSet) throws SQLException {
    return mDialect.parseResultSet(resultSet);
  }

}
