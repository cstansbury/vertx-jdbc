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
    final Statement statement = mTestConnection.createStatement();
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
    }
  }
  
  protected void tearDownTestDb() throws SQLException {
    final Statement statement = mTestConnection.createStatement();
    try {
      statement.execute("drop table user");
      System.out.println("Teardown complete!");
    } finally {
      statement.close();
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
      new JsonObject().put("sql", "select id, email, name, gender from user"), 
      EXECUTE_QUERY,
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
      EXECUTE_QUERY,
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
      EXECUTE_QUERY,
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
      EXECUTE_QUERY,
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
      EXECUTE_QUERY,
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
    assertResultSetNotExists("select * from user where email = 'mallory@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "insert into user(email, name, gender) values (?, ?, ?)")
        .put("bind", new JsonArray().add("mallory@test.com").add("Mallory").add("F"))
        .put("generatedKeys", true), 
      EXECUTE_UPDATE,
      (response) -> {
        System.out.println(response.cause());
        assertNotNull(response.result());
        System.out.println("Insert result: " + response.result().body());
        final JsonObject result = assertJsonObject(response.result().body(), 2);
        assertEquals(1, result.getInteger("rowCount").intValue());
        assertResultSetExists("select * from user where email = 'mallory@test.com'");
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
    assertResultSetNotExists("select * from user where email = 'mallory@test.com'");
    assertResultSetNotExists("select * from user where email = 'chuck@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "insert into user(email, name, gender) values (?, ?, ?)")
        .put("bind", new JsonArray()
          .add(new JsonArray().add("mallory@test.com").add("Mallory").add("F"))
          .add(new JsonArray().add("chuck@test.com").add("Chuck").add("M")))
        .put("generatedKeys", true), 
      EXECUTE_UPDATE,
      (response) -> {
        assertNotNull(response.result());
        final JsonArray results = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < results.size(); i++) {
          final JsonObject result = assertJsonObject(results.getValue(i), 2);
          assertEquals(1, result.getInteger("rowCount").intValue());
        }
        assertResultSetExists("select * from user where email = 'mallory@test.com'");
        assertResultSetExists("select * from user where email = 'chuck@test.com'");
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
      EXECUTE_UPDATE,
      (response) -> {
        assertNull(response.result());
        assertNotNull(response.cause());
        assertResultSetNotExists("select * from user where email = 'mallory@test.com'");
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
    assertResultSetNotExists("select * from user where name = 'Bobby'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "update user set name = ? where email = ?")
        .put("bind", new JsonArray().add("Bobby").add("bob@test.com")),
      EXECUTE_UPDATE,
      (response) -> {
        assertNotNull(response.result());
        final JsonObject result = assertJsonObject(response.result().body(), 1);
        assertEquals(1, result.getInteger("rowCount").intValue());
        assertResultSetExists("select * from user where name = 'Bobby'");
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
        .put("sql", "update user set name = ? where email = ?")
        .put("bind", new JsonArray().add("Bobby").add("bobaroni@test.com")),
      EXECUTE_UPDATE,
      (response) -> {
        assertNotNull(response.result());
        final JsonObject result = assertJsonObject(response.result().body(), 1);
        assertEquals(0, result.getInteger("rowCount").intValue());
        assertResultSetNotExists("select * from user where name = 'Bobby'");
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
    assertResultSetNotExists("select * from user where name = 'Bobby'");
    assertResultSetNotExists("select * from user where name = 'Alicia'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "update user set name = ? where email = ?")
        .put("bind", new JsonArray()
          .add(new JsonArray().add("Bobby").add("bob@test.com"))
          .add(new JsonArray().add("Alicia").add("alice@test.com"))), 
      EXECUTE_UPDATE,
      (response) -> {
        assertNotNull(response.result());
        final JsonArray results = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < results.size(); i++) {
          final JsonObject result = assertJsonObject(results.getValue(i), 1);
          assertEquals(1, result.getInteger("rowCount").intValue());
        }
        assertResultSetExists("select * from user where name = 'Bobby'");
        assertResultSetExists("select * from user where name = 'Alicia'");
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
        .put("sql", "update user set name = ? where email = ?")
        .put("bind", new JsonArray()
          .add(new JsonArray().add("Bobby").add("bobaroni@test.com"))
          .add(new JsonArray().add("Alicia").add("alicia@test.com"))), 
      EXECUTE_UPDATE,
      (response) -> {
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
        .put("sql", "update user set name = ? where email = ?")
        .put("bind", new JsonArray().add("Bobby")),
      EXECUTE_UPDATE,
      (response) -> {
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
    assertResultSetExists("select * from user where email = 'bob@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "delete from user where email = ?")
        .put("bind", new JsonArray().add("bob@test.com")),
      EXECUTE_UPDATE,
      (response) -> {
        assertNotNull(response.result());
        final JsonObject result = assertJsonObject(response.result().body(), 1);
        assertEquals(1, result.getInteger("rowCount").intValue());
        assertResultSetNotExists("select * from user where email = 'bob@test.com'");
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
    assertResultSetExists("select * from user where email = 'bob@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "delete from user where email = ?")
        .put("bind", new JsonArray().add("bobaroni@test.com")),
      EXECUTE_UPDATE,
      (response) -> {
        assertNotNull(response.result());
        final JsonObject result = assertJsonObject(response.result().body(), 1);
        assertEquals(0, result.getInteger("rowCount").intValue());
        assertResultSetExists("select * from user where email = 'bob@test.com'");
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
    assertResultSetExists("select * from user where email = 'bob@test.com'");
    assertResultSetExists("select * from user where email = 'alice@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "delete from user where email = ?")
        .put("bind", new JsonArray()
          .add(new JsonArray().add("bob@test.com"))
          .add(new JsonArray().add("alice@test.com"))), 
      EXECUTE_UPDATE,
      (response) -> {
        assertNotNull(response.result());
        final JsonArray results = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < results.size(); i++) {
          final JsonObject result = assertJsonObject(results.getValue(i), 1);
          assertEquals(1, result.getInteger("rowCount").intValue());
        }
        assertResultSetNotExists("select * from user where email = 'bob@test.com'");
        assertResultSetNotExists("select * from user where email = 'alice@test.com'");
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
    assertResultSetExists("select * from user where email = 'bob@test.com'");
    assertResultSetExists("select * from user where email = 'alice@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject()
        .put("sql", "delete from user where email = ?")
        .put("bind", new JsonArray()
          .add(new JsonArray().add("bobaroni@test.com"))
          .add(new JsonArray().add("alicia@test.com"))), 
      EXECUTE_UPDATE,
      (response) -> {
        assertNotNull(response.result());
        final JsonArray results = assertJsonArray(response.result().body(), 2);
        for (int i = 0; i < results.size(); i++) {
          final JsonObject result = assertJsonObject(results.getValue(i), 1);
          assertEquals(0, result.getInteger("rowCount").intValue());
        }
        assertResultSetExists("select * from user where email = 'bob@test.com'");
        assertResultSetExists("select * from user where email = 'alice@test.com'");
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
    assertResultSetExists("select * from user where email = 'bob@test.com'");
    vertx.eventBus().send(
      TESTDB_ADDRESS, 
      new JsonObject().put("sql", "delete from user where email = ?"),
      EXECUTE_UPDATE,
      (response) -> {
        assertNull(response.result());
        assertNotNull(response.cause());
        assertResultSetExists("select * from user where email = 'bob@test.com'");
        testComplete();
      }
    );
    await();
  }

  // -------------------------------------------------------------------------
  // Transaction Tests
  // -------------------------------------------------------------------------

//  /**
//   * 
//   */
//  @Test
//  public void test_transaction_insert_commit() {
//    assertResultSetNotExists("select * from user where email = 'mallory@test.com'");
//    System.out.println("Starting: " + System.currentTimeMillis());
//    vertx.eventBus().send(
//      TESTDB_ADDRESS, 
//      null,
//      START_TRANSACTION,
//      (startResponse) -> {
//        System.out.println("Finished Start");
//        assertTrue(startResponse.succeeded());
//        System.out.println("Inserting");
//        startResponse.result().reply(new JsonObject()
//            .put("sql", "insert into user(email, name, gender) values (?, ?, ?)")
//            .put("bind", new JsonArray().add("mallory@test.com").add("Mallory").add("F"))
//            .put("generatedKeys", true), 
//          EXECUTE_UPDATE,
//          (insertResponse) -> {
//            System.out.println("Finished Insert: " + insertResponse.result().body());
//            assertNotNull(insertResponse.result());
//            final JsonObject result = assertJsonObject(insertResponse.result().body(), 2);
//            assertEquals(1, result.getInteger("rowCount").intValue());
////            assertResultSetNotExists("select * from user where email = 'mallory@test.com'");
//            System.out.println("Committing");
//            insertResponse.result().reply(
//              null, 
//              COMMIT_TRANSACTION,
//              (commitResponse) -> {
//                System.out.println("Finished Commit");
//                assertTrue(commitResponse.succeeded());
//                assertResultSetExists("select * from user where email = 'mallory@test.com'");
//                System.out.println("Ending");
//                commitResponse.result().reply(
//                  null, 
//                  END_CONVERSATION,
//                  (endResponse) -> {
//                    System.out.println("Finished Ending: " + System.currentTimeMillis());
//                    testComplete();
//                  }
//                );
//              }
//            );
//          }
//        );
//      }
//    );
//    await();
//  }
  
//  /**
//   * 
//   */
//  @Test
//  public void test_transaction_insert_rollback() {
//    assertResultSetNotExists("select * from user where email = 'mallory@test.com'");
//    vertx.eventBus().send(TESTDB_ADDRESS, null, START_TRANSACTION, startResponse -> {
//        assertTrue(startResponse.succeeded());
//        startResponse.result().reply(new JsonObject()
//            .put("sql", "insert into user(email, name, gender) values (?, ?, ?)")
//            .put("bind", new JsonArray().add("mallory@test.com").add("Mallory").add("F"))
//            .put("generatedKeys", true), 
//          EXECUTE_UPDATE,
//          insertResponse -> {
//            assertNotNull(insertResponse.result());
//            final JsonObject result = assertJsonObject(insertResponse.result().body(), 2);
//            assertEquals(1, result.getInteger("rowCount").intValue());
////            assertResultSetNotExists("select * from user where email = 'mallory@test.com'");
//            insertResponse.result().reply(null, ROLLBACK_TRANSACTION, commitResponse -> {
//              assertTrue(commitResponse.succeeded());
//              assertResultSetNotExists("select * from user where email = 'mallory@test.com'");
//              commitResponse.result().reply(
//                null, 
//                END_CONVERSATION,
//                (endResponse) -> {
//                  testComplete();
//                }
//              );
//            });
//          }
//        );
//      }
//    );
//    await();
//  }
//  
//  /**
//   * 
//   */
//  @Test
//  public void test_transaction_insert_commit_future() {
//    final EventBusWrapper eventBus = FutureUtils.wrap(vertx.eventBus());
//    
//    eventBus.send(TESTDB_ADDRESS, START_TRANSACTION, null).thenCompose(message -> {
//      return message.sendReply(EXECUTE_UPDATE, new JsonObject()
//        .put("sql", "insert into user(email, name, gender) values (?, ?, ?)")
//        .put("bind", new JsonArray().add("mallory@test.com").add("Mallory").add("F"))
//        .put("generatedKeys", true)
//      );
//    }).thenCompose(message -> {
//      return message.sendReply(COMMIT_TRANSACTION);
//    }).thenCompose(message -> {
//      return message.sendReply(END_CONVERSATION);
//    }).thenAccept(message -> {
//      testComplete();
//    }).exceptionally(exception -> {
//      fail();
//      return null;
//    });
//    
//    await();
//  }

}
