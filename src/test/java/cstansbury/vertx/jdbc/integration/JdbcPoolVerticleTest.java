package cstansbury.vertx.jdbc.integration;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import cstansbury.vertx.jdbc.JdbcUtils;

public class JdbcPoolVerticleTest extends VertxTestBase {
  
  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  private static final String TESTDB_ADDRESS = "testdb";
  private static final String TESTDB_URL = "jdbc:hsqldb:mem:testdb";
  private static final String TESTDB_USER = "sa";
  private static final String TESTDB_PASSWORD = "";

  private static final DeliveryOptions EXECUTE_CALL = new DeliveryOptions().addHeader("action", "call");
  private static final DeliveryOptions EXECUTE_QUERY = new DeliveryOptions().addHeader("action", "query");
  private static final DeliveryOptions EXECUTE_UPDATE = new DeliveryOptions().addHeader("action", "update");

  // -------------------------------------------------------------------------
  // Member Variables
  // -------------------------------------------------------------------------

  private Connection mTestConnection;

  // -------------------------------------------------------------------------
  // Overridden VertxTestBase Protocol
  // -------------------------------------------------------------------------

  @Override
  public void setUp() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    super.setUp();
    setupTestDb();
    vertx.deployVerticle(
      "java:cstansbury.vertx.jdbc.JdbcPoolVerticle",
      new DeploymentOptions()
        .setMultiThreaded(true)
        .setWorker(true)
        .setConfig(new JsonObject()
          .put("address", TESTDB_ADDRESS)
          .put("pool", new JsonObject()
            .put("jdbcUrl", TESTDB_URL)
            .put("username", TESTDB_USER)
            .put("password", TESTDB_PASSWORD)
            .put("minimumIdle", 1)
          )
        ),
      (final AsyncResult<String> deployResult) -> {
        if (deployResult.succeeded()) {
          System.out.println("Deploy complete!");
        } else {
          System.out.println("Deploy error!");
          deployResult.cause().printStackTrace(System.err);
        }
        latch.countDown();
      }
    );
    awaitLatch(latch);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    tearDownTestDb();
  }
  
  // -------------------------------------------------------------------------
  // Setup / Teardown
  // -------------------------------------------------------------------------

  protected void setupTestDb() throws SQLException {
    mTestConnection = DriverManager.getConnection(TESTDB_URL, TESTDB_USER, TESTDB_PASSWORD);
    mTestConnection.setAutoCommit(true);
    try (final Statement statement = mTestConnection.createStatement()) {
      statement.execute("create table test_user ( " +
        "id integer generated always as identity(start with 1) primary key, " +
        "email varchar(30) not null, " +
        "name varchar(30), " +
        "gender char(1) not null " +
      ")");
      statement.executeUpdate("insert into test_user(email, name, gender) values('alice@test.com', 'Alice', 'F')");
      statement.executeUpdate("insert into test_user(email, name, gender) values('bob@test.com', 'Bob', 'M')");
      statement.executeUpdate("insert into test_user(email, name, gender) values('eve@test.com', 'Eve', 'F')");
      statement.execute("create procedure insert_test_user " +
        "(email varchar(30), name varchar(30), gender char(1), out total_users integer, inout echo_token varchar(32)) " +
        "modifies sql data dynamic result sets 1 " +
        "begin atomic " +
          "declare result cursor for select * from test_user where id = identity(); " +
          "insert into test_user values (DEFAULT, email, name, gender); " +
          "select count(*) into total_users from test_user; " +
          "set echo_token = reverse(echo_token); " +
          "open result; " +
        "end");
      System.out.println("Create & insert complete!");
    }
  }
  
  protected void tearDownTestDb() throws SQLException {
    try (final Statement statement = mTestConnection.createStatement()) {
      statement.execute("drop procedure insert_test_user");
      statement.execute("drop table test_user");
      System.out.println("Teardown complete!");
    } finally {
      mTestConnection.close();
    }
  }

  // -------------------------------------------------------------------------
  // Assert Utilities
  // -------------------------------------------------------------------------

  protected JsonObject assertJsonObject(final Object value) {
    assertNotNull(value);
    assertEquals(JsonObject.class, value.getClass());
    return (JsonObject) value;
  }

  protected JsonObject assertJsonObject(final Object value, final int size) {
    final JsonObject jsonObject = assertJsonObject(value);
    assertEquals(size, jsonObject.size());
    return jsonObject;
  }

  protected JsonArray assertJsonArray(final Object value) {
    assertNotNull(value);
    assertEquals(JsonArray.class, value.getClass());
    return (JsonArray) value;
  }

  protected JsonArray assertJsonArray(final Object value, final int size) {
    final JsonArray jsonArray = assertJsonArray(value);
    assertEquals(size, jsonArray.size());
    return jsonArray;
  }

  protected void assertResultSetExists(final String querySql) {
    Statement testStatement = null;
    ResultSet resultSet = null;
    try {
      testStatement = mTestConnection.createStatement();
      resultSet = testStatement.executeQuery(querySql);
      assertTrue(resultSet.next());
    } catch(SQLException e) {
      fail("Caught SQLException");
    } finally {
      JdbcUtils.closeQuietly(resultSet);
      JdbcUtils.closeQuietly(testStatement);
    }
  }

  protected void assertResultSetNotExists(final String querySql) {
    Statement testStatement = null;
    ResultSet resultSet = null;
    try {
      testStatement = mTestConnection.createStatement();
      resultSet = testStatement.executeQuery(querySql);
      assertFalse(resultSet.next());
    } catch(SQLException e) {
      fail("Caught SQLException");
    } finally {
      JdbcUtils.closeQuietly(resultSet);
      JdbcUtils.closeQuietly(testStatement);
    }
  }

  // -------------------------------------------------------------------------
  // Query Tests
  // -------------------------------------------------------------------------

  /**
   * 
   */
  @Test
  public void test_executeQuery_noBind() {
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject().put("sql", "select id, email, name, gender from test_user"), 
      EXECUTE_QUERY,
      response -> {
        assertNotNull(response.result());
        assertJsonArray(response.result().body(), 3);
        testComplete();
      }
    );
    await();
  }

  /**
   * 
   */
  @Test
  public void test_executeQuery_singleBind_singleParam() {
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "select id, email, name, gender from test_user where email = ?")
        .put("bind", new JsonArray().add("alice@test.com")), 
      EXECUTE_QUERY,
      response -> {
        assertNotNull(response.result());
        final JsonArray rows = assertJsonArray(response.result().body(), 1);
        final JsonObject row = assertJsonObject(rows.getValue(0), 4);
        assertEquals("alice@test.com", row.getString("email"));
        assertEquals("Alice", row.getString("name"));
        assertEquals("F", row.getString("gender"));
        testComplete();
      }
    );
    await();
  }
  
  /**
   * 
   */
  @Test
  public void test_executeQuery_singleBind_singleParam_multiResults() {
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "select id, email, name, gender from test_user where gender = ?")
        .put("bind", new JsonArray().add("F")), 
      EXECUTE_QUERY,
      response -> {
        assertNotNull(response.result());
        final JsonArray rows = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < rows.size(); i++) {
          final JsonObject row = assertJsonObject(rows.getValue(i), 4);
          assertEquals(i == 0 ? "alice@test.com" : "eve@test.com", row.getString("email"));
          assertEquals(i == 0 ? "Alice" : "Eve", row.getString("name"));
          assertEquals("F", row.getString("gender"));
        }
        testComplete();
      }
    );
    await();
  }
  
  /**
   * 
   */
  @Test
  public void test_executeQuery_singleBind_multiParam() {
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "select id, email, name, gender from test_user where name like ? and gender = ?")
        .put("bind", new JsonArray().add("Al%").add("F")), 
      EXECUTE_QUERY,
      response -> {
        assertNotNull(response.result());
        final JsonArray rows = assertJsonArray(response.result().body(), 1);
        final JsonObject row = assertJsonObject(rows.getValue(0), 4);
        assertEquals("alice@test.com", row.getString("email"));
        assertEquals("Alice", row.getString("name"));
        assertEquals("F", row.getString("gender"));
        testComplete();
      }
    );
    await();
  }
  
  /**
   * 
   */
  @Test
  public void test_executeQuery_multiBind_singleParam() {
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "select id, email, name, gender from test_user where email = ?")
        .put("bind", new JsonArray()
        .add(new JsonArray().add("alice@test.com"))
        .add(new JsonArray().add("eve@test.com"))
      ),
      EXECUTE_QUERY,
      response -> {
        assertNotNull(response.result());
        final JsonArray results = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < results.size(); i++) {
          final JsonArray rows = assertJsonArray(results.getValue(i), 1);
          final JsonObject row = assertJsonObject(rows.getValue(0), 4);
          assertEquals(i == 0 ? "alice@test.com" : "eve@test.com", row.getString("email"));
          assertEquals(i == 0 ? "Alice" : "Eve", row.getString("name"));
          assertEquals("F", row.getString("gender"));
        }
        testComplete();
      }
    );
    await();
  }

  // -------------------------------------------------------------------------
  // Insert Tests
  // -------------------------------------------------------------------------

  /**
   * 
   */
  @Test
  public void test_executeUpdate_insert_singleBind() {
    assertResultSetNotExists("select * from test_user where email = 'mallory@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "insert into test_user(email, name, gender) values (?, ?, ?)")
        .put("bind", new JsonArray().add("mallory@test.com").add("Mallory").add("F"))
        .put("generatedKeys", true), 
      EXECUTE_UPDATE,
      response -> {
        System.out.println(response.cause());
        assertNotNull(response.result());
        System.out.println("Insert result: " + response.result().body());
        final JsonObject result = assertJsonObject(response.result().body(), 2);
        assertEquals(1, result.getInteger("rowCount").intValue());
        assertResultSetExists("select * from test_user where email = 'mallory@test.com'");
        testComplete();
      }
    );
    await();
  }
  
  /**
   * 
   */
  @Test
  public void test_executeUpdate_insert_multipleBind() {
    assertResultSetNotExists("select * from test_user where email = 'mallory@test.com'");
    assertResultSetNotExists("select * from test_user where email = 'chuck@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "insert into test_user(email, name, gender) values (?, ?, ?)")
        .put("bind", new JsonArray()
          .add(new JsonArray().add("mallory@test.com").add("Mallory").add("F"))
          .add(new JsonArray().add("chuck@test.com").add("Chuck").add("M")))
        .put("generatedKeys", true), 
      EXECUTE_UPDATE,
      response -> {
        assertNotNull(response.result());
        final JsonArray results = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < results.size(); i++) {
          final JsonObject result = assertJsonObject(results.getValue(i), 2);
          assertEquals(1, result.getInteger("rowCount").intValue());
        }
        assertResultSetExists("select * from test_user where email = 'mallory@test.com'");
        assertResultSetExists("select * from test_user where email = 'chuck@test.com'");
        testComplete();
      }
    );
    await();
  }
  
  /**
   * 
   */
  @Test
  public void test_executeUpdate_insert_invalidBind() {
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "insert into test_user(email, name, gender) values (?, ?, ?, ?)")
        .put("bind", new JsonArray().add("mallory@test.com").add("Mallory").add("F"))
        .put("generatedKeys", true), 
      EXECUTE_UPDATE,
      response -> {
        assertNull(response.result());
        assertNotNull(response.cause());
        assertResultSetNotExists("select * from test_user where email = 'mallory@test.com'");
        testComplete();
      }
    );
    await();
  }
  
  // -------------------------------------------------------------------------
  // Update Tests
  // -------------------------------------------------------------------------

  /**
   * 
   */
  @Test
  public void test_executeUpdate_update_singleBind() {
    assertResultSetNotExists("select * from test_user where name = 'Bobby'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "update test_user set name = ? where email = ?")
        .put("bind", new JsonArray().add("Bobby").add("bob@test.com")),
      EXECUTE_UPDATE,
      response -> {
        assertNotNull(response.result());
        final JsonObject result = assertJsonObject(response.result().body(), 1);
        assertEquals(1, result.getInteger("rowCount").intValue());
        assertResultSetExists("select * from test_user where name = 'Bobby'");
        testComplete();
      }
    );
    await();
  }
  
  /**
   * 
   */
  @Test
  public void test_executeUpdate_update_singleBind_noMatch() {
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "update test_user set name = ? where email = ?")
        .put("bind", new JsonArray().add("Bobby").add("bobaroni@test.com")),
      EXECUTE_UPDATE,
      response -> {
        assertNotNull(response.result());
        final JsonObject result = assertJsonObject(response.result().body(), 1);
        assertEquals(0, result.getInteger("rowCount").intValue());
        assertResultSetNotExists("select * from test_user where name = 'Bobby'");
        testComplete();
      }
    );
    await();
  }

  /**
   * 
   */
  @Test
  public void test_executeUpdate_update_multipleBind() {
    assertResultSetNotExists("select * from test_user where name = 'Bobby'");
    assertResultSetNotExists("select * from test_user where name = 'Alicia'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "update test_user set name = ? where email = ?")
        .put("bind", new JsonArray()
          .add(new JsonArray().add("Bobby").add("bob@test.com"))
          .add(new JsonArray().add("Alicia").add("alice@test.com"))), 
      EXECUTE_UPDATE,
      response -> {
        assertNotNull(response.result());
        final JsonArray results = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < results.size(); i++) {
          final JsonObject result = assertJsonObject(results.getValue(i), 1);
          assertEquals(1, result.getInteger("rowCount").intValue());
        }
        assertResultSetExists("select * from test_user where name = 'Bobby'");
        assertResultSetExists("select * from test_user where name = 'Alicia'");
        testComplete();
      }
    );
    await();
  }
  
  /**
   * 
   */
  @Test
  public void test_executeUpdate_update_multipleBind_noMatches() {
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "update test_user set name = ? where email = ?")
        .put("bind", new JsonArray()
          .add(new JsonArray().add("Bobby").add("bobaroni@test.com"))
          .add(new JsonArray().add("Alicia").add("alicia@test.com"))), 
      EXECUTE_UPDATE,
      response -> {
        assertNotNull(response.result());
        final JsonArray results = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < results.size(); i++) {
          final JsonObject result = assertJsonObject(results.getValue(i), 1);
          assertEquals(0, result.getInteger("rowCount").intValue());
        }
        testComplete();
      }
    );
    await();
  }

  /**
   * 
   */
  @Test
  public void test_executeUpdate_update_invalidBind() {
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "update test_user set name = ? where email = ?")
        .put("bind", new JsonArray().add("Bobby")),
      EXECUTE_UPDATE,
      response -> {
        assertNull(response.result());
        assertNotNull(response.cause());
        testComplete();
      }
    );
    await();
  }

  // -------------------------------------------------------------------------
  // Delete Tests
  // -------------------------------------------------------------------------

  /**
   * 
   */
  @Test
  public void test_executeUpdate_delete_singleBind() {
    assertResultSetExists("select * from test_user where email = 'bob@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "delete from test_user where email = ?")
        .put("bind", new JsonArray().add("bob@test.com")),
      EXECUTE_UPDATE,
      response -> {
        assertNotNull(response.result());
        final JsonObject result = assertJsonObject(response.result().body(), 1);
        assertEquals(1, result.getInteger("rowCount").intValue());
        assertResultSetNotExists("select * from test_user where email = 'bob@test.com'");
        testComplete();
      }
    );
    await();
  }
  
  /**
   * 
   */
  @Test
  public void test_executeUpdate_delete_singleBind_noMatch() {
    assertResultSetExists("select * from test_user where email = 'bob@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "delete from test_user where email = ?")
        .put("bind", new JsonArray().add("bobaroni@test.com")),
      EXECUTE_UPDATE,
      response -> {
        assertNotNull(response.result());
        final JsonObject result = assertJsonObject(response.result().body(), 1);
        assertEquals(0, result.getInteger("rowCount").intValue());
        assertResultSetExists("select * from test_user where email = 'bob@test.com'");
        testComplete();
      }
    );
    await();
  }

  /**
   * 
   */
  @Test
  public void test_executeUpdate_delete_multipleBind() {
    assertResultSetExists("select * from test_user where email = 'bob@test.com'");
    assertResultSetExists("select * from test_user where email = 'alice@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "delete from test_user where email = ?")
        .put("bind", new JsonArray()
          .add(new JsonArray().add("bob@test.com"))
          .add(new JsonArray().add("alice@test.com"))), 
      EXECUTE_UPDATE,
      response -> {
        assertNotNull(response.result());
        final JsonArray results = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < results.size(); i++) {
          final JsonObject result = assertJsonObject(results.getValue(i), 1);
          assertEquals(1, result.getInteger("rowCount").intValue());
        }
        assertResultSetNotExists("select * from test_user where email = 'bob@test.com'");
        assertResultSetNotExists("select * from test_user where email = 'alice@test.com'");
        testComplete();
      }
    );
    await();
  }
  
  /**
   * 
   */
  @Test
  public void test_executeUpdate_delete_multipleBind_noMatches() {
    assertResultSetExists("select * from test_user where email = 'bob@test.com'");
    assertResultSetExists("select * from test_user where email = 'alice@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "delete from test_user where email = ?")
        .put("bind", new JsonArray()
          .add(new JsonArray().add("bobaroni@test.com"))
          .add(new JsonArray().add("alicia@test.com"))), 
      EXECUTE_UPDATE,
      response -> {
        assertNotNull(response.result());
        final JsonArray results = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < results.size(); i++) {
          final JsonObject result = assertJsonObject(results.getValue(i), 1);
          assertEquals(0, result.getInteger("rowCount").intValue());
        }
        assertResultSetExists("select * from test_user where email = 'bob@test.com'");
        assertResultSetExists("select * from test_user where email = 'alice@test.com'");
        testComplete();
      }
    );
    await();
  }

  /**
   * 
   */
  @Test
  public void test_executeUpdate_delete_invalidBind() {
    assertResultSetExists("select * from test_user where email = 'bob@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject().put("sql", "delete from test_user where email = ?"),
      EXECUTE_UPDATE,
      response -> {
        assertNull(response.result());
        assertNotNull(response.cause());
        assertResultSetExists("select * from test_user where email = 'bob@test.com'");
        testComplete();
      }
    );
    await();
  }

  // -------------------------------------------------------------------------
  // Call Tests
  // -------------------------------------------------------------------------

  /**
   * 
   */
  @Test
  public void test_executeCall_singleBind() {
    assertResultSetNotExists("select * from test_user where email = 'mallory@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "{call insert_test_user(?, ?, ?, ?, ?)}")
        .put("bind", new JsonArray()
          .add(new JsonObject().put("in", "mallory@test.com"))
          .add(new JsonObject().put("in", "Mallory"))
          .add(new JsonObject().put("in", "F"))
          .add(new JsonObject().put("out", Types.INTEGER).put("name", "rowCount"))
          .add(new JsonObject().put("in", "ping").put("out", Types.VARCHAR).put("name", "echoToken"))
        ),
      EXECUTE_CALL,
      response -> {
        assertNotNull(response.result());
        final JsonObject result = assertJsonObject(response.result().body(), 3);
        assertEquals(4, result.getInteger("rowCount").intValue());
        assertEquals("gnip", result.getString("echoToken"));
        assertResultSetExists("select * from test_user where email = 'mallory@test.com'");
        testComplete();
      }
    );
    await();
  }
  

}
