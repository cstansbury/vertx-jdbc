package io.vertx.ext.jdbc.integration;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

public class JdbcPoolVerticleTest extends VertxTestBase {
  
  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  private static final String TESTDB_ADDRESS = "testdb";
  private static final String TESTDB_URL = "jdbc:hsqldb:mem:testdb";
  private static final String TESTDB_USER = "sa";
  private static final String TESTDB_PASSWORD = "";

  // -------------------------------------------------------------------------
  // Overridden VertxTestBase Protocol
  // -------------------------------------------------------------------------

  @Override
  public void setUp() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    super.setUp();
    setupTestDb();
    vertx.deployVerticle(
      "java:io.vertx.ext.jdbc.JdbcPoolVerticle",
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
    final Connection connection = DriverManager.getConnection(TESTDB_URL, TESTDB_USER, TESTDB_PASSWORD);
    final Statement statement = connection.createStatement();
    try {
      statement.execute("create table user ( " +
        "id integer generated always as identity(start with 1) primary key, " +
        "email varchar(30) not null, " +
        "name varchar(30), " +
        "gender char(1) not null " +
      ")");
      statement.executeUpdate("insert into user(email, name, gender) values('alice@test.com', 'Alice', 'F')");
      statement.executeUpdate("insert into user(email, name, gender) values('bob@test.com', 'Bob', 'M')");
      statement.executeUpdate("insert into user(email, name, gender) values('eve@test.com', 'Eve', 'F')");
      System.out.println("Create & insert complete!");
    } finally {
      statement.close();
      connection.close();
    }
  }
  
  protected void tearDownTestDb() throws SQLException {
    final Connection connection = DriverManager.getConnection(TESTDB_URL, TESTDB_USER, TESTDB_PASSWORD);
    final Statement statement = connection.createStatement();
    try {
      statement.execute("drop table user");
      System.out.println("Teardown complete!");
    } finally {
      statement.close();
      connection.close();
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
      new JsonObject().put("sql", "select id, email, name, gender from user"), 
      new DeliveryOptions().addHeader("action", "executeQuery"),
      (response) -> {
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
        .put("sql", "select id, email, name, gender from user where email = ?")
        .put("bind", new JsonArray().add("alice@test.com")), 
      new DeliveryOptions().addHeader("action", "executeQuery"),
      (response) -> {
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
        .put("sql", "select id, email, name, gender from user where gender = ?")
        .put("bind", new JsonArray().add("F")), 
      new DeliveryOptions().addHeader("action", "executeQuery"),
      (response) -> {
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
        .put("sql", "select id, email, name, gender from user where name like ? and gender = ?")
        .put("bind", new JsonArray().add("Al%").add("F")), 
      new DeliveryOptions().addHeader("action", "executeQuery"),
      (response) -> {
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
        .put("sql", "select id, email, name, gender from user where email = ?")
        .put("bind", new JsonArray()
        .add(new JsonArray().add("alice@test.com"))
        .add(new JsonArray().add("eve@test.com"))
      ),
      new DeliveryOptions().addHeader("action", "executeQuery"),
      (response) -> {
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
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "insert into user(email, name, gender) values (?, ?, ?)")
        .put("bind", new JsonArray().add("mallory@test.com").add("Mallory").add("F"))
        .put("generatedKeys", true), 
      new DeliveryOptions().addHeader("action", "executeUpdate"),
      (response) -> {
        System.out.println(response.cause());
        assertNotNull(response.result());
        System.out.println("Insert result: " + response.result().body());
        final JsonObject result = assertJsonObject(response.result().body(), 2);
        assertEquals(1, result.getInteger("rowCount").intValue());
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
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "insert into user(email, name, gender) values (?, ?, ?)")
        .put("bind", new JsonArray()
          .add(new JsonArray().add("mallory@test.com").add("Mallory").add("F"))
          .add(new JsonArray().add("chuck@test.com").add("Chuck").add("M")))
        .put("generatedKeys", true), 
      new DeliveryOptions().addHeader("action", "executeUpdate"),
      (response) -> {
        assertNotNull(response.result());
        final JsonArray results = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < results.size(); i++) {
          final JsonObject result = assertJsonObject(results.getValue(i), 2);
          assertEquals(1, result.getInteger("rowCount").intValue());
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
  public void test_executeUpdate_insert_invalidBind() {
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "insert into user(email, name, gender) values (?, ?, ?, ?)")
        .put("bind", new JsonArray().add("mallory@test.com").add("Mallory").add("F"))
        .put("generatedKeys", true), 
      new DeliveryOptions().addHeader("action", "executeUpdate"),
      (response) -> {
        assertNull(response.result());
        assertNotNull(response.cause());
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
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "update user set name = ? where email = ?")
        .put("bind", new JsonArray().add("Bobby").add("bob@test.com")),
      new DeliveryOptions().addHeader("action", "executeUpdate"),
      (response) -> {
        assertNotNull(response.result());
        final JsonObject result = assertJsonObject(response.result().body(), 1);
        assertEquals(1, result.getInteger("rowCount").intValue());
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
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "update user set name = ? where email = ?")
        .put("bind", new JsonArray()
          .add(new JsonArray().add("Bobby").add("bob@test.com"))
          .add(new JsonArray().add("Alicia").add("alice@test.com"))), 
      new DeliveryOptions().addHeader("action", "executeUpdate"),
      (response) -> {
        assertNotNull(response.result());
        final JsonArray results = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < results.size(); i++) {
          final JsonObject result = assertJsonObject(results.getValue(i), 1);
          assertEquals(1, result.getInteger("rowCount").intValue());
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
        .put("sql", "update user set name = ? where email = ?")
        .put("bind", new JsonArray().add("Bobby")),
      new DeliveryOptions().addHeader("action", "executeUpdate"),
      (response) -> {
        assertNull(response.result());
        assertNotNull(response.cause());
        testComplete();
      }
    );
    await();
  }

}
