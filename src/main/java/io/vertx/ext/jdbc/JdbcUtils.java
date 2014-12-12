package io.vertx.ext.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcUtils {

  public static void closeQuietly(final ResultSet resultSet) {
    if (resultSet != null) { 
      try { resultSet.close(); } catch(SQLException ignored) { } 
    }
  }

  public static void closeQuietly(final Statement statement) {
    if (statement != null) { 
      try { statement.close(); } catch(SQLException ignored) { } 
    }
  }

  public static void closeQuietly(final Connection connection) {
    if (connection != null) { 
      try { connection.close(); } catch(SQLException ignored) { } 
    }
  }

}
