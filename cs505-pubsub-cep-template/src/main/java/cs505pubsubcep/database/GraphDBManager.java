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
import java.util.*;

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
    public  Map<Integer,OVertex> hospitals; //map that takes in hospital id as a key and returns the vertex from the database

    public GraphDBManager() {

        try {
            hospitals = new HashMap<>();
            System.out.println("ESTABLISHING CONNECTION");
            orientdb = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
            // orientdb.create("COVID", ODatabaseType.PLOCAL);
            session = orientdb.open("COVID", "admin", "admin");
            System.out.println("CONNECTION ESTABLISHED");
            // createSchema();
            // System.out.println("CREATED SCHEMA");
            //
            // populateHospitalsFromFile();
            // System.out.println("POPULATED HOSPITALS");


            session.close();
            orientdb.close();
            resetDB();

            System.out.println("DATABASE RESET SUCCESS");

        }

        catch (Exception ex) {
            System.out.println("CONSTRUCTOR FAILED");
            ex.printStackTrace();
        }

    }

    private void createSchema(){
      OClass patient = session.getClass("Patient");
      OClass hospital = session.getClass("Hospital");
      OClass zipcodedistance = session.getClass("ZipCodeDistance");


      try {

        //Check if classes exist
        if (patient == null) {
              patient = session.createVertexClass("Patient");
        }

        if (hospital == null) {
              hospital = session.createVertexClass("Hospital");
        }

        if (zipcodedistance == null) {
              zipcodedistance = session.createVertexClass("ZipCodeDistance");
        }

        //Check if patient properties exist
        if (patient.getProperty("mrn") == null) {
              patient.createProperty("mrn", OType.STRING);
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

        if (patient.getProperty("location_code") == null) {
              patient.createProperty("location_code", OType.STRING);
              patient.createIndex("patient_location_code_index", OClass.INDEX_TYPE.NOTUNIQUE, "location_code");
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
        if (zipcodedistance.getProperty("zip_code_1") == null) {
              zipcodedistance.createProperty("zip_code_1", OType.INTEGER);
              zipcodedistance.createIndex("zip_code_1_index", OClass.INDEX_TYPE.NOTUNIQUE, "zip_code_1");
        }
        if (zipcodedistance.getProperty("zip_code_2") == null) {
              zipcodedistance.createProperty("zip_code_2", OType.INTEGER);
              zipcodedistance.createIndex("zip_code_2_index", OClass.INDEX_TYPE.NOTUNIQUE, "zip_code_2");
        }
        if (zipcodedistance.getProperty("distance") == null) {
              zipcodedistance.createProperty("distance", OType.INTEGER);
              zipcodedistance.createIndex("zip_distance_index", OClass.INDEX_TYPE.NOTUNIQUE, "distance");
        }
      }

      catch (Exception ex) {
          System.out.println("RESET GRAPH DATABASE FAILED");
          ex.printStackTrace();
      }
    }

    public void resetDB(){
      try {
        orientdb = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
        // orientdb.create("COVID", ODatabaseType.PLOCAL);
        session = orientdb.open("COVID", "admin", "admin");

        session.close();
        orientdb.drop("COVID");
        hospitals.clear();
        System.out.println("DROPPED DATABASE");
        orientdb.create("COVID", ODatabaseType.PLOCAL);
        session = orientdb.open("COVID", "admin", "admin");
        System.out.println("CREATED AND OPENED DATABASE");
        createSchema();
        populateZipCodesFromFile();
        populateHospitalsFromFile();

        session.close();
        orientdb.close();

      }

      catch (Exception ex) {
          System.out.println("RESET GRAPH DATABASE FAILED");
          ex.printStackTrace();
      }
    }

    private void populateHospitalsFromFile(){
      String csvFile = "src/main/java/cs505pubsubcep/provideddata/hospitals.csv";

      CSVReader csvReader = null;

      try {
        Reader reader = Files.newBufferedReader(Paths.get(csvFile));
        csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();
        String[] line;
        while ((line = csvReader.readNext()) != null) {
          System.out.println("id= " + line[0] + ", total_beds= " + line[7] + " , zip_code=" + line[5]);
          createHospital(line[0], line[7], line[5]);
        }
      }
      catch (Exception ex) {
          System.out.println("READ HOSPITALS FAILED");
          ex.printStackTrace();
      }
    }


    private void createHospital(String id, String total_beds, String zip_code) {
      try {
        OVertex result = session.newVertex("Hospital");
        result.setProperty("id", Integer.parseInt(id));
        result.setProperty("alert_status", 0);
        result.setProperty("total_beds", Integer.parseInt(total_beds));
        result.setProperty("available_beds", Integer.parseInt(total_beds));
        result.setProperty("zip_code", Integer.parseInt(zip_code));
        // OEdge edge = result.addEdge(zip_code, "LocatedAt");
        // edge.save();
        result.save();
        hospitals.put(Integer.parseInt(id), result);
      }
      catch (Exception ex) {
          System.out.println("CREATE HOSPITAL FAILED");
          ex.printStackTrace();
      }

    }

    public void createPatient(String mrn, int zip_code, int status) {
      try {
        orientdb = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
        session = orientdb.open("COVID", "admin", "admin");

        int location_code = -1;
        OVertex result = session.newVertex("Patient");
        result.setProperty("mrn", mrn);
        result.setProperty("zip_code", zip_code);
        result.setProperty("status", status);
        // result.setProperty("location_code", location_code);

        if (status == 2 || status == 5 || status == 6){ //if the patient tested positive (is sick)
          //Query for a hospital
          String query = "select * from hospital where zip_code = (select zip_code_2 from ZipCodeDistance where zip_code_1 = ? and zip_code_2 in (select zip_code from Hospital where available_beds > 0) order by distance asc limit 1 )order by available_beds desc limit 1";
          OResultSet rs = session.query(query, zip_code);
          OResult or = rs.next();
          location_code = or.getProperty("id");
          OVertex hospital = hospitals.get(location_code);
          hospital.setProperty("available_beds", ((int) hospital.getProperty("available_beds"))-1);
          hospitals.replace(location_code, hospital);
          hospital.save();
        }

        result.setProperty("location_code", location_code);

        result.save();
        
        session.close();
        orientdb.close();
      }
      catch (Exception ex) {
          System.out.println("CREATE PATIENT FAILED");
          ex.printStackTrace();
      }

    }

    private void populateZipCodesFromFile(){
      String csvFile = "src/main/java/cs505pubsubcep/provideddata/kyzipdistance.csv";

      CSVReader csvReader = null;

      try {
        Reader reader = Files.newBufferedReader(Paths.get(csvFile));
        csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();
        String[] line;
        Set<Integer> zips = new HashSet<Integer>();
        while ((line = csvReader.readNext()) != null) {
          System.out.println("zip 1= " + line[0] + ", zip 2= " + line[1] + " , distance=" + line[2]);
          int zipfrom = Integer.parseInt(line[0]);
          int zipto = Integer.parseInt(line[1]);
          float distance = Float.parseFloat(line[2]);
          zips.add(zipfrom);
          createZipCodeDistance(zipfrom, zipto, distance);
        }
        for(Integer i : zips)
          createZipCodeDistance(i, i, 0);
      }
      catch (Exception ex) {
          System.out.println("READ ZIPCODES FAILED");
          ex.printStackTrace();
      }
    }

    private void createZipCodeDistance(int zip1, int zip2, float distance) {
      try {
        OVertex result = session.newVertex("ZipCodeDistance");
        result.setProperty("zip_code_1", zip1);
        result.setProperty("zip_code_2", zip2);
        result.setProperty("distance", distance);
        result.save();
      }
      catch (Exception ex) {
          System.out.println("CREATE ZIPCODEDISTANCE FAILED");
          ex.printStackTrace();
      }

    }

}
