package cs505pubsubcep.database;

import com.google.gson.reflect.TypeToken;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.*;


public class DBEngine {

    private DataSource ds;

    public DBEngine() {

        try {
            //Name of database
            String databaseName = "myDatabase";

            //Driver needs to be identified in order to load the namespace in the JVM
            String dbDriver = "org.apache.derby.jdbc.EmbeddedDriver";
            Class.forName(dbDriver).newInstance();

            //Connection string pointing to a local file location
            String dbConnectionString = "jdbc:derby:memory:" + databaseName + ";create=true";
            ds = setupDataSource(dbConnectionString);

            //if the database does not exist, create it and all tables
            /*
            if(!databaseExist(databaseName)) {
                System.out.println("No database, creating " + databaseName);
                initDB();
            } else {
                System.out.println("Database found, removing " + databaseName);
                delete(Paths.get(databaseName).toFile());
                System.out.println("Creating " + databaseName);
                initDB();
            }
            */

            initDB();


        }

        catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static DataSource setupDataSource(String connectURI) {
        //
        // First, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
        ConnectionFactory connectionFactory = null;
            connectionFactory = new DriverManagerConnectionFactory(connectURI, null);


        //
        // Next we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);

        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);

        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        PoolingDataSource<PoolableConnection> dataSource =
                new PoolingDataSource<>(connectionPool);

        return dataSource;
    }

    public void initDB() {

        String createRNode = "CREATE TABLE zipcodes" +
                "(" +
                "   zip_code int," +
                "   positives int," +
                "   alert smallint" +
                ")";

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(createRNode);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    public int executeUpdate(String stmtString) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeUpdate(stmtString);
                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return  result;
    }

    public int dropTable(String tableName) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                String stmtString = null;

                stmtString = "DROP TABLE " + tableName;

                Statement stmt = conn.createStatement();

                result = stmt.executeUpdate(stmtString);

                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    /*
    public boolean databaseExist(String databaseName)  {
        return Paths.get(databaseName).toFile().exists();
    }
    */
    public boolean databaseExist(String databaseName)  {
        boolean exist = false;
        try {

            if(!ds.getConnection().isClosed()) {
                exist = true;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public boolean tableExist(String tableName)  {
        boolean exist = false;

            ResultSet result;
            DatabaseMetaData metadata = null;

            try {
                metadata = ds.getConnection().getMetaData();
                result = metadata.getTables(null, null, tableName.toUpperCase(), null);

                if(result.next()) {
                    exist = true;
                }
            } catch(java.sql.SQLException e) {
                e.printStackTrace();
            }

        catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public List<Map<String,String>> getAccessLogs() {
        List<Map<String,String>> accessMapList = null;
        try {

            accessMapList = new ArrayList<>();

            Type type = new TypeToken<Map<String, String>>(){}.getType();

            String queryString = null;

            //fill in the query
            queryString = "SELECT * FROM accesslog";

            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        while (rs.next()) {
                            Map<String, String> accessMap = new HashMap<>();
                            accessMap.put("remote_ip", rs.getString("remote_ip"));
                            accessMap.put("access_ts", rs.getString("access_ts"));
                            accessMapList.add(accessMap);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return accessMapList;
    }

    public boolean zipCodeExists(String zipCode) {
      boolean exist = false;
      try {
        String queryString = "SELECT * FROM zipcodes";
        try(Connection conn = ds.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try(ResultSet rs = stmt.executeQuery(queryString)) {
                  while (rs.next()) {
                      if (rs.getString("zip_code").equals(zipCode)) {
                        exist = true;
                      }
                  }
                }
            }
        }
    } catch(Exception ex) {
        ex.printStackTrace();
    }
    return exist;
  }

    public Map<String,Integer> getZipCodes() {
      Map<String, Integer> accessMap = new HashMap<>();
      try {
        String queryString = "SELECT * FROM zipcodes";
        try(Connection conn = ds.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try(ResultSet rs = stmt.executeQuery(queryString)) {
                    while (rs.next()) {
                        accessMap.put(rs.getString("zip_code"), rs.getInt("positives"));
                    }
                }
            }
        }
    } catch(Exception ex) {
        ex.printStackTrace();
    }
    return accessMap;
  }

  public Set<String> getAlertedZipCodes() {
    Set<String> zipCodes = new HashSet<String>();
    try {
      String queryString = "SELECT * FROM zipcodes WHERE alert = 1";
      try(Connection conn = ds.getConnection()) {
          try (Statement stmt = conn.createStatement()) {
              try(ResultSet rs = stmt.executeQuery(queryString)) {
                  while (rs.next()) {
                    // if (rs.getInt("alert") == 1) {
                    //   zipCodes.add(rs.getString("zip_code"));
                    // }
                    zipCodes.add(rs.getString("zip_code"));

                  }
              }
          }
      }
  } catch(Exception ex) {
      ex.printStackTrace();
  }
  return zipCodes;
  }
}
