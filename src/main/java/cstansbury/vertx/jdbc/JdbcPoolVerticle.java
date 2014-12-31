package cstansbury.vertx.jdbc;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import cstansbury.vertx.jdbc.dialect.BaseJdbcDialect;

/**
 * 
 * @author cstansbury
 */
public class JdbcPoolVerticle extends AbstractVerticle implements Handler<Message<JsonObject>> {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------
  
  protected static final String DEFAULT_ADDRESS = "jdbc-pool";
  
  // -------------------------------------------------------------------------
  // Member Variables
  // -------------------------------------------------------------------------

  private HikariDataSource mDataSource;
  
  private JdbcDialect mDialect;
  
  // -------------------------------------------------------------------------
  // Overridden AbstractVerticle Protocol
  // -------------------------------------------------------------------------

  @Override
  public void start(final Future<Void> startFuture) throws Exception {
    mDataSource = new HikariDataSource(getPoolConfig());
    mDialect = new BaseJdbcDialect();
    vertx.eventBus().consumer(config().getString("address", DEFAULT_ADDRESS), this);
    startFuture.complete();
  }
  
  // -------------------------------------------------------------------------
  // Overridden Handler Protocol
  // -------------------------------------------------------------------------

  @Override
  public void handle(final Message<JsonObject> message) {
    final JsonObject requestBody = message.body();
    final String action = message.headers().get("action");
    Object responseBody = null;
      
    try (final Connection connection = mDataSource.getConnection()){
      if ("query".equals(action)) {
        responseBody = mDialect.executeQuery(connection, requestBody); 
      } else if ("update".equals(action)) {
        responseBody = mDialect.executeUpdate(connection, requestBody);           
      } else if ("call".equals(action)) {
        responseBody = mDialect.executeCall(connection, requestBody); 
      } else {
        message.fail(0, "Invalid action: " + action);
      }
    } catch (final SQLException e) {
      message.fail(e.getErrorCode(), e.getMessage());
    }
    
    if (responseBody != null) {
      message.reply(responseBody);
    }
  }

  // -------------------------------------------------------------------------
  // Protected Protocol
  // -------------------------------------------------------------------------

  protected HikariConfig getPoolConfig() {
    final JsonObject config = config().getJsonObject("pool");
    final Properties configProperties = new Properties();
    
    configProperties.putAll(config.getMap());
    
    return new HikariConfig(configProperties);
  }
  
}
