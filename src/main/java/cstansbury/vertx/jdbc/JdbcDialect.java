package cstansbury.vertx.jdbc;

import io.vertx.core.json.JsonObject;

import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcDialect {

  Object executeCall(Connection connection, JsonObject requestBody) throws SQLException;
  
  Object executeQuery(Connection connection, JsonObject requestBody) throws SQLException;

  Object executeUpdate(Connection connection, JsonObject requestBody) throws SQLException;

}
