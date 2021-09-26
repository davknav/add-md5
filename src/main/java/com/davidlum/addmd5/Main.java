package com.davidlum.addmd5;

import java.sql.*;

public class Main {
  private static int BATCH_SIZE = 250;

  public static void main(String[] args) throws Exception {
    Connection conn = getConnection();
    int highestIdSoFar = -1;
    do {
      highestIdSoFar = doBatch(conn, highestIdSoFar);
    } while (highestIdSoFar > 0);
  }
  private static int doBatch(Connection conn, int highestIdSoFar) throws Exception {
    try (PreparedStatement queryStmt = conn.prepareStatement(
            "select id, object_uuid, object_uuid_md5 from dpkg_object where id > ? order by id limit ?",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
      queryStmt.setInt(1, highestIdSoFar);
      queryStmt.setInt(2, BATCH_SIZE);
      try (ResultSet rs = queryStmt.executeQuery()) {
        return doUpdates(conn, rs);
      }
    }
  }

  private static int doUpdates(Connection conn, ResultSet rs) throws Exception {
    int id = -1;
    try (PreparedStatement updateStmt = conn.prepareStatement(
            "update dpkg_object set object_uuid_md5=? where id = ?")) {
      while (rs.next()) {
        id = rs.getInt(1);
        String objectUuid = rs.getString(2);
        updateStmt.setString(1, "foo" + id);
        updateStmt.setInt(2, id);
        updateStmt.addBatch();
      }
      updateStmt.executeBatch();
      return id;
    }
  }

  private static void print(String msg) {
    System.out.println(msg);
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
