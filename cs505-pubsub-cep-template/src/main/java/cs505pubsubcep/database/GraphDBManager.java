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
import java.util.Optional;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
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

    private OrientDB orientdb;
    private ODatabasePool pool;
    private String dbname;
    public boolean isOpen;

    public GraphDBManager() {

        try {
            dbname = "COVID";
            orientdb = new OrientDB("remote:jda268.cs.uky.edu","root","rootpwd", OrientDBConfig.defaultConfig());
            isOpen = false;
            openDatabase();
            ODatabaseSession session = pool.acquire();
            createSchema(session);
            session.activateOnCurrentThread();
            session.close();
            resetDB();
            // populateZipCodesFromFile();
            // populateHospitalsFromFile();
            System.out.println("CONSTRUCTOR SUCCESS");
            // session.close();
            closeDatabase();

        }

        catch (Exception ex) {
            System.out.println("CONSTRUCTOR FAILED");
            ex.printStackTrace();
        }

    }

    private void createSchema(ODatabaseSession db){

      try {

        OClass patient = db.getClass("Patient");
        OClass hospital = db.getClass("Hospital");
        OClass zipcodedistance = db.getClass("ZipCodeDistance");

        //Check if classes exist
        if (patient == null) {
              patient = db.createVertexClass("Patient");
        }

        if (hospital == null) {
              hospital = db.createVertexClass("Hospital");
        }

        if (zipcodedistance == null) {
              zipcodedistance = db.createVertexClass("ZipCodeDistance");
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

        OResultSet hospitals = db.query("Select 1 from Hospital");
        if(!hospitals.hasNext()){ //if there are no hospitals
          System.out.println("POPULATING HOSPITALS");
          populateHospitalsFromFile();
        }
        else
          db.command("UPDATE Hospital SET available_beds = total_beds");

        db.activateOnCurrentThread();

        OResultSet zipcodes = db.query("Select 1 from ZipCodeDistance");
        if(!zipcodes.hasNext()){ //if there are no zipcodedistances
          System.out.println("POPULATING ZIPCODES");
          populateZipCodesFromFile();
        }
      }

      catch (Exception ex) {
          System.out.println("CREATE SCHEMA FAILED");
          ex.printStackTrace();
      }
    }

    public void resetDB(){
      boolean wasOpen = isOpen;
      openDatabase();
      try(ODatabaseSession db = pool.acquire()) {
        db.command("DROP CLASS Patient UNSAFE");
        System.out.println("DROPPED PATIENTS");
        createSchema(db);
        db.close();
      }

      catch (Exception ex) {
          System.out.println("RESET GRAPH DATABASE FAILED");
          ex.printStackTrace();
      }
      finally{
        if(!wasOpen)
          closeDatabase();
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
      // session = pool.acquire();
      try(ODatabaseSession db = pool.acquire()) {
        OVertex result = db.newVertex("Hospital");
        result.setProperty("id", Integer.parseInt(id));
        result.setProperty("alert_status", 0);
        result.setProperty("total_beds", Integer.parseInt(total_beds));
        result.setProperty("available_beds", Integer.parseInt(total_beds));
        result.setProperty("zip_code", Integer.parseInt(zip_code));
        result.save();
        db.close();
      }
      catch (Exception ex) {
          System.out.println("CREATE HOSPITAL FAILED");
          ex.printStackTrace();
      }
      // finally{
      //   session.close();
      // }

    }

    public void createPatient(String mrn, int zip_code, int status) {
      // session = pool.acquire();
      try(ODatabaseSession db = pool.acquire()) {

        OVertex result = db.newVertex("Patient");
        result.setProperty("mrn", mrn);
        result.setProperty("zip_code", zip_code);
        result.setProperty("status", status);

        if(status == 0 || status == 1 || status == 2 || status == 4){
          result.setProperty("location_code", 0);
        }
        else if (status == 3 || status == 5 || status == 6){ //if the patient tested positive (is sick)
          //Query for a hospital
          String query = "select * from hospital where zip_code = (select zip_code_2 from ZipCodeDistance where zip_code_1 = ? and zip_code_2 in (select zip_code from Hospital where available_beds > 0) order by distance asc limit 1 )order by available_beds desc limit 1";
          OResultSet rs = db.query(query, zip_code);
          //Check if results are empty
          //get neighboring zipcodes in order of asc distance
          //get fisrt hospital in one of those zipcodes

          //check if the results are empty
          if (rs.hasNext()){
            rs.next().getVertex().ifPresent(x->{
                   final int hospital_location_code = x.getProperty("id");
                   result.setProperty("location_code", hospital_location_code);
                   x.setProperty("available_beds", ((int) x.getProperty("available_beds"))-1);
                   x.save();
                 });
          }
          else
            result.setProperty("location_code", -1);
        }
        else
          result.setProperty("location_code", -1);

        result.save();
        db.close();
      }
      catch (Exception ex) {
          System.out.println("CREATE PATIENT FAILED");
          ex.printStackTrace();
      }
      // finally{
      //   session.close();
      // }

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
      // session = pool.acquire();
      try(ODatabaseSession db = pool.acquire()) {
        OVertex result = db.newVertex("ZipCodeDistance");
        result.setProperty("zip_code_1", zip1);
        result.setProperty("zip_code_2", zip2);
        result.setProperty("distance", distance);
        result.save();
        db.close();
      }
      catch (Exception ex) {
          System.out.println("CREATE ZIPCODEDISTANCE FAILED");
          ex.printStackTrace();
      }
      // finally{
      //   session.close();
      // }

    }

    public void closeDatabase(){
       // session.close();
       if(isOpen){
         pool.close();
         // orientdb.close();
         System.out.println("CONNECTION CLOSED");
         isOpen = false;
       }

     }

    public void openDatabase() {

       if(orientdb.exists(dbname)){
         if(!isOpen){
           System.out.println("ESTABLISHING CONNECTION");

           orientdb.open(dbname, "admin", "admin");

           OrientDBConfigBuilder poolCfg = OrientDBConfig.builder();
           poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MIN, 5);
           poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MAX, 200);
           pool = new ODatabasePool(orientdb, dbname,"admin","admin", poolCfg.build());
           isOpen = true;
           System.out.println("CONNECTION ESTABLISHED");
         }
         else
          System.out.println("DATABASE ALREADY OPEN");
       }
       else{
         isOpen = false;
         orientdb.create(dbname, ODatabaseType.PLOCAL);
         System.out.println("DATABASE CREATED");
         openDatabase();
       }


      }

    public String getPatientLocation(String mrn){
      // session = pool.acquire();
      try(ODatabaseSession db = pool.acquire()) {
        // System.out.println("MRN: " + mrn);
        System.out.println("CHECKING FOR " + mrn);
        String query = "select * from Patient where mrn = ?";
        OResultSet rs = db.query(query, mrn);
        db.close();

        //check if the results are empty
        if (rs.hasNext()){
          OResult patient = rs.next();
          return patient.getProperty("location_code");
          // rs.next().getVertex().ifPresent(x->{
          //       final String location = x.getProperty("location_code");
          //        return location;
          //      });
        }

      }
      catch (Exception ex) {
          System.out.println("GET PATIENT FAILED");
          ex.printStackTrace();
      }
      finally{
      }
      return "-1";
    }

    public int[] getHospitalInfo(int hospital_id){
      int hospital_info[] = new int[3];
      try(ODatabaseSession db = pool.acquire()) {
        String query = "select * from Hospital where id = ?";
        OResultSet rs = db.query(query, hospital_id);
        db.close();

        //check if the results are empty
        if (rs.hasNext()){
          OResult hospital = rs.next();
          hospital_info[0] = hospital.getProperty("total_beds");
          hospital_info[1] = hospital.getProperty("available_beds");
          hospital_info[2] = hospital.getProperty("zip_code");
        }

      }
      catch (Exception ex) {
          System.out.println("GET HOSPITAL FAILED");
          hospital_info[0] = -1;
          hospital_info[1] = -1;
          hospital_info[2] = -1;
          ex.printStackTrace();
      }
      finally{
        return hospital_info;
      }
    }


}
