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

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import com.opencsv.CSVReader;
import java.io.FileReader;
import com.opencsv.CSVReaderBuilder;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;


public class GraphDBManager {

    private ODatabaseSession session;
    private OrientDB orientdb;

    public GraphDBManager() {

        try {
            System.out.println("ESTABLISHING CONNECTION");
            orientdb = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
            // orientdb.create("COVID", ODatabaseType.PLOCAL);
            session = orientdb.open("COVID", "admin", "admin");
            System.out.println("CONNECTION ESTABLISHED");
            resetDB();
            System.out.println("DATABASE RESET SUCCESS");


            // createSchema();
            // System.out.println("CREATED SCHEMA");
            //
            // populateHospitalsFromFile();
            // System.out.println("POPULATED HOSPITALS");


            session.close();
            orientdb.close();

        }

        catch (Exception ex) {
            System.out.println("CONSTRUCTOR FAILED");
            ex.printStackTrace();
        }

    }

    private void createSchema(){
      OClass patient = session.getClass("Patient");
      OClass hospital = session.getClass("Hospital");
      OClass zipcode = session.getClass("ZipCode");


      try {

        //Check if classes exist
        if (patient == null) {
              patient = session.createVertexClass("Patient");
        }

        if (hospital == null) {
              hospital = session.createVertexClass("Hospital");
        }

        if (zipcode == null) {
              zipcode = session.createVertexClass("ZipCode");
        }

        //Check if patient properties exist
        if (patient.getProperty("mrn") == null) {
              patient.createProperty("mrn", OType.INTEGER);
              patient.createIndex("patient_mrn_index", OClass.INDEX_TYPE.NOTUNIQUE, "mrn");
        }

        if (patient.getProperty("zip_code") == null) {
              patient.createProperty("zip_code", OType.INTEGER);
              patient.createIndex("patient_zip_code_index", OClass.INDEX_TYPE.NOTUNIQUE, "zip_code");
        }

        if (patient.getProperty("status") == null) {
              patient.createProperty("status", OType.INTEGER);
              patient.createIndex("patient_status_index", OClass.INDEX_TYPE.NOTUNIQUE, "status");
        }

        //Check if hospital properties exist
        if (hospital.getProperty("id") == null) {
              hospital.createProperty("id", OType.INTEGER);
              hospital.createIndex("hospital_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "id");
        }

        if (hospital.getProperty("alert_status") == null) {
              hospital.createProperty("alert_status", OType.INTEGER);
              hospital.createIndex("hospital_status_index", OClass.INDEX_TYPE.NOTUNIQUE, "alert_status");
        }

        if (hospital.getProperty("total_beds") == null) {
              hospital.createProperty("total_beds", OType.INTEGER);
              hospital.createIndex("hospital_total_beds_index", OClass.INDEX_TYPE.NOTUNIQUE, "total_beds");
        }

        if (hospital.getProperty("available_beds") == null) {
              hospital.createProperty("available_beds", OType.INTEGER);
              hospital.createIndex("hospital_available_beds_index", OClass.INDEX_TYPE.NOTUNIQUE, "available_beds");
        }

        if (hospital.getProperty("zip_code") == null) {
              hospital.createProperty("zip_code", OType.INTEGER);
              hospital.createIndex("hospital_zip_code_index", OClass.INDEX_TYPE.NOTUNIQUE, "zip_code");
        }

        //Check if zipcode properties exist
        if (zipcode.getProperty("id") == null) {
              zipcode.createProperty("id", OType.INTEGER);
              zipcode.createIndex("zipcode_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "id");
        }

        //Create Edge classes
        if (session.getClass("PatientAt") == null) {
             session.createEdgeClass("PatientAt");
         }

        if (session.getClass("DistanceFrom") == null) {
              session.createEdgeClass("DistanceFrom");
        }

        if (session.getClass("LocatedAt") == null) {
               session.createEdgeClass("LocatedAt");
         }
      }

      catch (Exception ex) {
          System.out.println("RESET GRAPH DATABASE FAILED");
          ex.printStackTrace();
      }
    }

    public void resetDB(){
      try {
        session.close();
        orientdb.drop("COVID");
        orientdb.create("COVID", ODatabaseType.PLOCAL);
        session = orientdb.open("COVID", "admin", "admin");
        createSchema();
        Map<Integer,OVertex> zipCodes = populateZipCodesFromFile();
        populateHospitalsFromFile(zipCodes);
      }

      catch (Exception ex) {
          System.out.println("RESET GRAPH DATABASE FAILED");
          ex.printStackTrace();
      }
    }

    private void populateHospitalsFromFile(Map<Integer,OVertex> zipCodes){
      String csvFile = "src/main/java/cs505pubsubcep/provideddata/hospitals.csv";

      CSVReader csvReader = null;

      try {
        Reader reader = Files.newBufferedReader(Paths.get(csvFile));
        csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();
        String[] line;
        while ((line = csvReader.readNext()) != null) {
          System.out.println("id= " + line[0] + ", total_beds= " + line[7] + " , zip_code=" + line[5]);
          createHospital(line[0], line[7], zipCodes.get(Integer.parseInt(line[5])));
        }
      }
      catch (Exception ex) {
          System.out.println("READ HOSPITALS FAILED");
          ex.printStackTrace();
      }
    }


    private void createHospital(String id, String total_beds, OVertex zip_code) {
      try {
        OVertex result = session.newVertex("Hospital");
        result.setProperty("id", Integer.parseInt(id));
        result.setProperty("alert_status", 0);
        result.setProperty("total_beds", Integer.parseInt(total_beds));
        result.setProperty("available_beds", Integer.parseInt(total_beds));
        // result.setProperty("zip_code", Integer.parseInt(zip_code));
        OEdge edge = result.addEdge(zip_code, "LocatedAt");
        edge.save();
        result.save();
      }
      catch (Exception ex) {
          System.out.println("CREATE HOSPITAL FAILED");
          ex.printStackTrace();
      }

    }

    private Map<Integer,OVertex> populateZipCodesFromFile(){
      String csvFile = "src/main/java/cs505pubsubcep/provideddata/kyzipdistance.csv";

      CSVReader csvReader = null;

      try {
        Reader reader = Files.newBufferedReader(Paths.get(csvFile));
        csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();
        String[] line;
        Map<Integer,OVertex> zipCodes = new HashMap<>();
        while ((line = csvReader.readNext()) != null) {
          System.out.println("zip 1= " + line[0] + ", zip 2= " + line[1] + " , distance=" + line[2]);
          int zipfrom = Integer.parseInt(line[0]);
          int zipto = Integer.parseInt(line[1]);
          float distance = Float.parseFloat(line[2]);
          //Check if zip 1 exists
          if(!zipCodes.containsKey(zipfrom))
            zipCodes.put(zipfrom, createZipCode(zipfrom));
          //Check if zip 2 exists
          if(!zipCodes.containsKey(zipto))
            zipCodes.put(zipto, createZipCode(zipto));
          //create edge between zips 1 and 2
          OEdge edge = zipCodes.get(zipfrom).addEdge(zipCodes.get(zipto), "DistanceFrom");
          edge.setProperty("distance", distance);
          edge.save();
        }
        return zipCodes;
      }
      catch (Exception ex) {
          System.out.println("READ ZIPCODES FAILED");
          ex.printStackTrace();
          return null;
      }
    }

    private OVertex createZipCode(int id) {
      try {
        OVertex result = session.newVertex("ZipCode");
        result.setProperty("id", id);
        result.save();
        return result;
      }
      catch (Exception ex) {
          System.out.println("CREATE ZIPCODE FAILED");
          ex.printStackTrace();
          return null;
      }

    }

}
