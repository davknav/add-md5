package com.davidlum.addmd5;

import java.sql.*;

public class Main {
  public static void main(String[] args) throws Exception {
    method7();
  }

  private static void print(String msg) {
    System.out.println(msg);
  }

  /** Results in about 400 network packets in Wireshark */
  private static void method1() throws Exception {
    Connection conn = getConnection();
    PreparedStatement stmt = conn.prepareStatement("select id, object_uuid from dpkg_object",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    stmt.setFetchSize(500);
    ResultSet rs = stmt.executeQuery();
    while (rs.next()) {
      int id = rs.getInt(1);
      String objectUuid = rs.getString(2);
      print("^^^ row: " + id + ", " + objectUuid);
    }
  }

  /** Results in about 5,100 network packets in Wireshark */
  private static void method2() throws Exception {
    Connection conn = getConnection();
    int batchSize = 250;
    int highestIdSoFar = -1;
    try (PreparedStatement stmt = conn.prepareStatement(
            "select id, object_uuid, object_uuid_md5 from dpkg_object where id > ? order by id limit ?",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
      boolean didUpdate;
      do {
        didUpdate = false;
        stmt.setInt(1, highestIdSoFar);
        stmt.setInt(2, batchSize);
        try (ResultSet rs = stmt.executeQuery()) {
          int id = highestIdSoFar;
          while (rs.next()) {
            id = rs.getInt(1);
            String objectUuid = rs.getString(2);
            print("^^^ row: " + id + ", " + objectUuid);
            rs.updateString(3, "foo");
            rs.updateRow();
            didUpdate = true;
          }
          highestIdSoFar = id;
        }
      } while (didUpdate);
      conn.commit();
    }
  }

  /** Results in about 5,200 network packets in Wireshark */
  private static void method3() throws Exception {
    Connection conn = getConnection();
    int batchSize = 250;
    int highestIdSoFar = -1;
    boolean didUpdate;
    do {
      didUpdate = false;
      try (PreparedStatement stmt = conn.prepareStatement(
              "select id, object_uuid, object_uuid_md5 from dpkg_object where id > ? order by id limit ?",
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
        stmt.setInt(1, highestIdSoFar);
        stmt.setInt(2, batchSize);
        try (ResultSet rs = stmt.executeQuery()) {
          int id = highestIdSoFar;
          while (rs.next()) {
            id = rs.getInt(1);
            String objectUuid = rs.getString(2);
            print("^^^ row: " + id + ", " + objectUuid);
            rs.updateString(3, "foo");
            rs.updateRow();
            didUpdate = true;
          }
          highestIdSoFar = id;
        }
      }
      conn.commit();
    } while (didUpdate);
  }

  /** Results in about 3,000 network packets in Wireshark */
  private static void method4() throws Exception {
    Connection conn = getConnection();
    int batchSize = 250;
    int highestIdSoFar = -1;
    try (PreparedStatement queryStmt = conn.prepareStatement(
            "select id, object_uuid, object_uuid_md5 from dpkg_object where id > ? order by id limit ?",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

         PreparedStatement updateStmt = conn.prepareStatement(
                 "update dpkg_object set object_uuid_md5=? where id = ?")) {
      boolean didUpdate;
      do {
        didUpdate = false;
        queryStmt.setInt(1, highestIdSoFar);
        queryStmt.setInt(2, batchSize);
        try (ResultSet rs = queryStmt.executeQuery()) {
          int id = highestIdSoFar;
          conn.setAutoCommit(false);
          while (rs.next()) {
            id = rs.getInt(1);
            String objectUuid = rs.getString(2);
            print("^^^ row: " + id + ", " + objectUuid);
            updateStmt.setString(1, "foo" + id);
            updateStmt.setInt(2, id);
            updateStmt.addBatch();
            didUpdate = true;
          }
          highestIdSoFar = id;
        }
        updateStmt.executeBatch();
        conn.commit();
      } while (didUpdate);
    }
  }

  /** Results in about 2,500 network packets in Wireshark */
  private static void method5() throws Exception {
    Connection conn = getConnection();
    int batchSize = 250;
    int highestIdSoFar = -1;

    boolean didUpdate;
    do {
      didUpdate = false;
      try (PreparedStatement queryStmt = conn.prepareStatement(
              "select id, object_uuid, object_uuid_md5 from dpkg_object where id > ? order by id limit ?",
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

           PreparedStatement updateStmt = conn.prepareStatement(
                   "update dpkg_object set object_uuid_md5=? where id = ?")) {
        queryStmt.setInt(1, highestIdSoFar);
        queryStmt.setInt(2, batchSize);
        try (ResultSet rs = queryStmt.executeQuery()) {
          int id = highestIdSoFar;
          conn.setAutoCommit(false);
          while (rs.next()) {
            id = rs.getInt(1);
            String objectUuid = rs.getString(2);
            print("^^^ row: " + id + ", " + objectUuid);
            updateStmt.setString(1, "foo" + id);
            updateStmt.setInt(2, id);
            updateStmt.addBatch();
            didUpdate = true;
          }
          highestIdSoFar = id;
        }
        updateStmt.executeBatch();
        conn.commit();
      }
    } while (didUpdate);
  }

  private static void method6() throws Exception {
    Connection conn = getConnection();
    Statement stmt = conn.createStatement();
    conn.setAutoCommit(false);
    stmt.addBatch("update dpkg_object set object_uuid_md5='foo0' where id = 0");
    stmt.addBatch("update dpkg_object set object_uuid_md5='foo1' where id = 1");
    stmt.addBatch("update dpkg_object set object_uuid_md5='foo2' where id = 2");
    stmt.addBatch("update dpkg_object set object_uuid_md5='foo3' where id = 3");
    stmt.addBatch("update dpkg_object set object_uuid_md5='foo4' where id = 4");
    stmt.addBatch("update dpkg_object set object_uuid_md5='foo5' where id = 5");
    stmt.addBatch("update dpkg_object set object_uuid_md5='foo6' where id = 6");
    stmt.addBatch("update dpkg_object set object_uuid_md5='foo7' where id = 7");
    stmt.addBatch("update dpkg_object set object_uuid_md5='foo8' where id = 8");
    stmt.addBatch("update dpkg_object set object_uuid_md5='foo9' where id = 9");
    int[] ints = stmt.executeBatch();
    conn.commit();
    System.out.println("^^^ Affected " + ints.length + " rows");
  }

  private static void method7() throws Exception {
    Connection conn = getConnection();
    int highestIdSoFar = -1;
    do {
      highestIdSoFar = doBatch7(conn, highestIdSoFar);
    } while (highestIdSoFar > 0);
  }

  private static int doBatch7(Connection conn, int highestIdSoFar) throws Exception {
    int BATCH_SIZE = 250;
    try (PreparedStatement queryStmt = conn.prepareStatement(
            "select id, object_uuid, object_uuid_md5 from dpkg_object where id > ? order by id limit ?",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
        queryStmt.setInt(1, highestIdSoFar);
        queryStmt.setInt(2, BATCH_SIZE);
      try (ResultSet rs = queryStmt.executeQuery()) {
        return doResultSet7(conn, rs);
      }
    }
  }

  private static int doResultSet7(Connection conn, ResultSet rs) throws Exception {
    int id = -1;
    try (PreparedStatement updateStmt = conn.prepareStatement(
            "update dpkg_object set object_uuid_md5=? where id = ?")) {
      while (rs.next()) {
        id = rs.getInt(1);
        String objectUuid = rs.getString(2);
        print("^^^ row: " + id + ", " + objectUuid);
        updateStmt.setString(1, "foo" + id);
        updateStmt.setInt(2, id);
        updateStmt.addBatch();
      }
      updateStmt.executeBatch();
      return id;
    }
  }

  private static Connection getConnection() throws Exception {
    StringBuffer url = new StringBuffer("jdbc:mysql://localhost:3306/test?");
    url.append("logger=Slf4JLogger"); // Enable log messages
    url.append("&");
    url.append("profileSQL=true"); // ???
    url.append("&");
    url.append("useSSL=false&requireSSL=false"); // Use plaintext so we can snoop with Wireshark
    url.append("&");
    url.append("rewriteBatchedStatements=true");

    Connection conn = DriverManager.getConnection(url.toString(), "dlum", "dlum");
    DatabaseMetaData dbMetaData = conn.getMetaData();
    boolean supportsBatchUpdates = dbMetaData.supportsBatchUpdates();
    if (!supportsBatchUpdates) {
      throw new Exception("ERROR: Database doesn't support batch updates");
    }
    return conn;
  }
}
