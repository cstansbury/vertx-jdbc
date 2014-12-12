package io.vertx.ext.jdbc;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface JdbcDialect {

  void applyBindParams(PreparedStatement statement, JsonArray bindParams) throws SQLException;

  JsonObject parseResultSet(ResultSet resultSet) throws SQLException;
  
}
