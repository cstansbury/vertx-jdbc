package cstansbury.vertx.jdbc;

import io.vertx.core.json.JsonObject;

import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcAction {

  Object execute(Connection connection, JsonObject requestBody) throws SQLException;
  
}
