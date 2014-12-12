package io.vertx.ext.jdbc;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.dialect.BaseJdbcDialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

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
  // Protected Statement Parameter Binding Utilities
  // -------------------------------------------------------------------------

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
  
  protected PreparedStatement applyBindParams(final PreparedStatement statement, final JsonArray bindParams) throws SQLException {
    mDialect.applyBindParams(statement, bindParams);
    return statement;
  }

  // -------------------------------------------------------------------------
  // Protected ResultSet Utilities
  // -------------------------------------------------------------------------

  protected JsonObject parseResultSet(final ResultSet resultSet) throws SQLException {
    return mDialect.parseResultSet(resultSet);
  }
  
  // -------------------------------------------------------------------------
  // Inner Handler Classes
  // -------------------------------------------------------------------------

  protected class MessageHandler implements Handler<Message<JsonObject>> {

    private Connection mConnection;

    private Boolean mAutoCommit;
    
    private boolean mInConversation;
    
    @Override
    public void handle(final Message<JsonObject> message) {
      final JsonObject requestBody = message.body();
      Object responseBody = null;

      try {
        final String action = message.headers().get("action");
        
        if (action != null) {
          switch (action) {
            case "execute":
              message.fail(0, "Action 'execute' not yet implemented");
              break;
            case "executeCall":
              message.fail(0, "Action 'executeCall' not yet implemented");
              break;
            case "executeQuery":
              responseBody = executeQuery(requestBody);
              break;
            case "executeUpdate":
              responseBody = executeUpdate(requestBody);
              break;
            case "startTransaction":
              ensureConnection();
              mAutoCommit = mConnection.getAutoCommit();
              mConnection.setAutoCommit(false);
              // fall through
            case "startConversation":
              mInConversation = true;
              break;
            case "commitTransaction":
              assertInTransaction();
              mConnection.commit();
              resetAutoCommit();
              break;
            case "rollbackTransaction":
              assertInTransaction();
              mConnection.rollback();
              resetAutoCommit();
              break;
            case "endConversation":
              mInConversation = false;
              break;
            default:
              message.fail(0, "Invalid action: " + action);
          }
        }

        if (mInConversation) {
          message.reply(responseBody, new ReplyHandler());
        } else {
          message.reply(responseBody);
        }
      } catch (final IllegalStateException e) {
        message.fail(0, e.getMessage());
      } catch (final SQLException e) {
        message.fail(e.getErrorCode(), e.getMessage());
      } finally {
        if (!mInConversation) {
          closeConnection();
        }
      }
    }

    public void assertInTransaction() {
      if (mAutoCommit == null) {
        throw new IllegalStateException("Not in transaction");
      }
    }
    
    protected void resetAutoCommit() throws SQLException {
      if (mAutoCommit != null) {
        mConnection.setAutoCommit(mAutoCommit);
        mAutoCommit = null;
      }
    }

    protected Object executeQuery(final JsonObject requestBody) throws SQLException {
      final PreparedStatement statement = prepareStatement(requestBody);
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

    protected Object executeUpdate(final JsonObject requestBody) throws SQLException {
      final PreparedStatement statement = prepareStatement(requestBody);
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
    
    protected PreparedStatement prepareStatement(final JsonObject requestBody) throws SQLException {
      final String sql = requestBody.getString("sql");
      PreparedStatement statement = null;
      
      if (sql == null) {
        // todo throw exception
      }
      
      // TODO lookup in statement cache?

      final Boolean generatedKeys = requestBody.getBoolean("generatedKeys");
      final JsonArray generatedKeyIndices = requestBody.getJsonArray("generatedKeyIndices");

      ensureConnection();

      if (Boolean.TRUE.equals(generatedKeys)) {
        statement = mConnection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
      } else if (generatedKeyIndices != null) {
        final int[] columnIndexes = new int[generatedKeyIndices.size()];
        for (int i = 0; i < columnIndexes.length; i++) {
          columnIndexes[i] = generatedKeyIndices.getInteger(i);
        }
        statement = mConnection.prepareStatement(sql, columnIndexes);
      } else {
        statement = mConnection.prepareStatement(sql);
      }
      
      return statement;
    }

    protected void closeConnection() {
      JdbcUtils.closeQuietly(mConnection);
      mConnection = null;
    }

    protected Connection ensureConnection() throws SQLException {
      if (mConnection == null) {
        mConnection = mDataSource.getConnection();
      }
      return mConnection;
    }
   
    protected class ReplyHandler implements Handler<AsyncResult<Message<JsonObject>>> {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> event) {
        Message<JsonObject> message = event.result();
        if (message != null) {
          MessageHandler.this.handle(message);
        } else {
          try { resetAutoCommit(); } catch(final SQLException ignored) { }
          closeConnection();
          mInConversation = false;
        }
      }
    }
    
  }

}
