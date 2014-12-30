package cstansbury.vertx.jdbc;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import cstansbury.vertx.jdbc.action.JdbcQueryAction;
import cstansbury.vertx.jdbc.action.JdbcUpdateAction;
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
  
  protected static final JsonArray EMPTY_JSON_ARRAY = new JsonArray();
  
  // -------------------------------------------------------------------------
  // Member Variables
  // -------------------------------------------------------------------------

  private HikariDataSource mDataSource;
  
  private JdbcDialect mDialect;
  
  private Map<String, JdbcAction> mActions;

  // -------------------------------------------------------------------------
  // Overridden AbstractVerticle Protocol
  // -------------------------------------------------------------------------

  @Override
  @SuppressWarnings("serial")
  public void start(final Future<Void> startFuture) throws Exception {
    mDataSource = new HikariDataSource(getPoolConfig());
    mDialect = new BaseJdbcDialect();
    mActions = new HashMap<String, JdbcAction>() {{
      put("executeQuery", new JdbcQueryAction(mDialect));
      put("executeUpdate", new JdbcUpdateAction(mDialect));
    }};
    vertx.eventBus().consumer(config().getString("address", DEFAULT_ADDRESS), this);
    startFuture.complete();
  }
  
  // -------------------------------------------------------------------------
  // Overridden Handler Protocol
  // -------------------------------------------------------------------------

  @Override
  public void handle(final Message<JsonObject> message) {
    new MessageHandler().handle(message);
  }

  // -------------------------------------------------------------------------
  // Protected Config Protocol
  // -------------------------------------------------------------------------

  protected HikariConfig getPoolConfig() {
    final JsonObject config = config().getJsonObject("pool");
    final Properties configProperties = new Properties();
    
    configProperties.putAll(config.getMap());
    
    return new HikariConfig(configProperties);
  }
  
  // -------------------------------------------------------------------------
  // Inner Handler Classes
  // -------------------------------------------------------------------------

  /**
   * 
   */
  protected class MessageHandler implements Handler<Message<JsonObject>> {

    @Override
    public void handle(final Message<JsonObject> message) {
      final JsonObject requestBody = message.body();
      final String actionValue = message.headers().get("action");
      final JdbcAction action = mActions.get(actionValue);

      if (actionValue != null) {
        Connection connection = null;
        try {
          connection = mDataSource.getConnection();
          message.reply(action.execute(connection, requestBody));
        } catch (final SQLException e) {
          message.fail(e.getErrorCode(), e.getMessage());
        } finally {
          JdbcUtils.closeQuietly(connection);
        }
      } else {
        message.fail(0, "Invalid action: " + actionValue);
      }
    }

  }

}
