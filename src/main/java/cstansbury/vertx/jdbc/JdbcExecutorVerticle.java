/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cstansbury.vertx.jdbc;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import cstansbury.vertx.jdbc.JdbcRequest.CommitStatus;
import cstansbury.vertx.jdbc.dialect.BaseJdbcDialect;

/**
 * 
 * @author cstansbury
 */
public class JdbcExecutorVerticle extends AbstractVerticle implements Handler<Message<JsonObject>> {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------
  
  protected static final String DEFAULT_ADDRESS = "jdbc-executor";
  
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

    if (requestBody == null) {
      message.fail(0, "Missing request body");
    } else if (requestBody.getString("sql") == null) {
      message.fail(0, "Missing request body SQL");
    } else {
      try (final Connection connection = mDataSource.getConnection()) {
        JdbcRequest jdbcRequest = new JdbcRequest(action, requestBody, connection);
        try {
          responseBody = handle(message, jdbcRequest);
        } catch(final Exception e) {
          if (jdbcRequest.getCommitStatus() == CommitStatus.ON) {
            connection.rollback();
          }
          throw e;
        }
      } catch (final SQLException e) {
        message.fail(e.getErrorCode(), e.getMessage());
      }
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
  
  protected Object handle(final Message<JsonObject> message, final JdbcRequest request) throws SQLException {
    final String action = request.getAction();
    Object responseBody = null;
    
    if ("query".equals(action)) {
      responseBody = mDialect.executeQuery(request); 
    } else if ("update".equals(action)) {
      responseBody = mDialect.executeUpdate(request);           
    } else if ("call".equals(action)) {
      responseBody = mDialect.executeCall(request);
    } else if ("batch".equals(action)) {
      responseBody = handleBatch(message, request);
    } else {
      message.fail(0, "Invalid action: " + action);
    }
    
    return responseBody;
  }

  protected Object handleBatch(final Message<JsonObject> message, final JdbcRequest jdbcRequest) throws SQLException {
    final JsonArray batchResponse = new JsonArray();
//    final JsonArray batch = jdbcRequest.getBody().getJsonArray("batch");
//    
//    for (int i = 0; i < batch.size(); i++) {
//      final JsonObject request = batch.getJsonObject(i);
//      final String action = request.getString("action");
//      handle(message, new JdbcRequest(action, body, jdbcRequest.getConnection()));
//    }
//    
    return batchResponse;
  }

}
