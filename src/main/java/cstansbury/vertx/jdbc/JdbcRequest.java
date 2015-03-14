package cstansbury.vertx.jdbc;

import io.vertx.core.json.JsonObject;

import java.sql.Connection;

public class JdbcRequest {

  // -------------------------------------------------------------------------
  // Enums
  // -------------------------------------------------------------------------

  public enum CommitStatus { OFF, ON, ROLLBACK };
  
  // -------------------------------------------------------------------------
  // Member Variables
  // -------------------------------------------------------------------------

  private final String mAction;
  private final JsonObject mBody;
  private final Connection mConnection;
  private CommitStatus mCommitStatus;
  
  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------

  public JdbcRequest(final String action, final JsonObject body, final Connection connection) {
    this(action, body, connection, CommitStatus.OFF);
  }

  public JdbcRequest(final String action, final JsonObject body, final Connection connection, final CommitStatus commitStatus) {
    mConnection = connection;
    mAction = action;
    mBody = body;
    mCommitStatus = commitStatus;
  }

  // -------------------------------------------------------------------------
  // Member Accessors
  // -------------------------------------------------------------------------

  public Connection getConnection() {
    return mConnection;
  }

  public String getAction() {
    return mAction;
  }
  
  public JsonObject getBody() {
    return mBody;
  }

  public CommitStatus getCommitStatus() {
    return mCommitStatus;
  }

  public void setCommitStatus(final CommitStatus commitStatus) {
    mCommitStatus = commitStatus;
  }
  
}
