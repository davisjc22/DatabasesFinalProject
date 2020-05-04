package cs505pubsubcep.httpcontrollers;

import com.google.gson.Gson;
import cs505pubsubcep.CEP.accessRecord;
import cs505pubsubcep.Launcher;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.*;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public static Integer NUM_OF_POSITIVES = 0;
    public static Integer NUM_OF_NEGATIVES = 0;

    public API() {
        gson = new Gson();
    }

    //check local
    //curl --header "X-Auth-API-key:1234" "http://localhost:8082/api/checkmycep"

    //check remote
    //curl --header "X-Auth-API-key:1234" "http://[linkblueid].cs.uky.edu:8082/api/checkmycep"
    //curl --header "X-Auth-API-key:1234" "http://localhost:8081/api/checkmycep"

    //check remote
    //curl --header "X-Auth-API-key:1234" "http://[linkblueid].cs.uky.edu:8081/api/checkmycep"

    @GET
    @Path("/checkmycep")
    @Produces(MediaType.APPLICATION_JSON)
    public Response checkMyEndpoint() {
        String responseString = "{}";
        try {

            //get remote ip address from request
            String remoteIP = request.get().getRemoteAddr();
            //get the timestamp of the request
            long access_ts = System.currentTimeMillis();
            System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

            Map<String,String> responseMap = new HashMap<>();
            if(Launcher.cepEngine != null) {

                    responseMap.put("success", Boolean.TRUE.toString());
                    responseMap.put("status_desc","CEP Engine exists");

            } else {
                responseMap.put("success", Boolean.FALSE.toString());
                responseMap.put("status_desc","CEP Engine is null!");
            }

            responseString = gson.toJson(responseMap);


        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getaccesscount")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAccessCount() {
        String responseString = "{}";
        try {

            //get remote ip address from request
            String remoteIP = request.get().getRemoteAddr();
            //get the timestamp of the request
            long access_ts = System.currentTimeMillis();
            System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

            //generate event based on access
            String inputEvent = gson.toJson(new accessRecord(remoteIP,access_ts));
            System.out.println("inputEvent: " + inputEvent);

            //send input event to CEP
            Launcher.cepEngine.input(Launcher.inputStreamName, inputEvent);

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("accesscoint",String.valueOf(Launcher.accessCount));
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

   @GET
   @Path("/getteam")
   @Produces(MediaType.APPLICATION_JSON)
   public Response getTeam() {
     String responseString = "{}";
     try {

       //get remote ip address from request
       String remoteIP = request.get().getRemoteAddr();
       //get the timestamp of the request
       long access_ts = System.currentTimeMillis();
       System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

       String[] student_ids = new String[] { "12111623", "12110132" };

       Map<String, Object> responseMap = new HashMap<>();
       if(Launcher.cepEngine != null) {
               responseMap.put("team_name", "fixItNow");
               responseMap.put("Team_members_sids", student_ids);
               responseMap.put("app_status_code", "1");

       } else {
         responseMap.put("team_name", "fixItNow");
         responseMap.put("Team_members_sids", student_ids);
         responseMap.put("app_status_code", "0");
       }
       responseString = gson.toJson(responseMap);
     } catch (Exception ex) {

         StringWriter sw = new StringWriter();
         ex.printStackTrace(new PrintWriter(sw));
         String exceptionAsString = sw.toString();
         ex.printStackTrace();

         return Response.status(500).entity(exceptionAsString).build();
     }
     return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
   }

  @GET
  @Path("/reset")
  @Produces(MediaType.APPLICATION_JSON)
  public Response reset() {
    String responseString = "{}";
    try {

      //get remote ip address from request
      String remoteIP = request.get().getRemoteAddr();
      //get the timestamp of the request
      long access_ts = System.currentTimeMillis();
      System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

      Map<String,String> responseMap = new HashMap<>();
      try {
        // reset static counters and reset embedded derby db
        NUM_OF_POSITIVES = 0;
        NUM_OF_NEGATIVES = 0;
        Launcher.dbEngine.dropTable("zipcodes");
        Launcher.dbEngine.initDB();
        Launcher.dbManager.resetDB();

        // if resetting things does not throw exception, return a successful 1 response
        responseMap.put("reset_status_code", "1");
      } catch (Exception ex) {
        // if an exception  is caught at any point, return a 0
        responseMap.put("reset_status_code", "0");
      }

      responseString = gson.toJson(responseMap);

    } catch (Exception ex) {

        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        String exceptionAsString = sw.toString();
        ex.printStackTrace();

        return Response.status(500).entity(exceptionAsString).build();
    }
    return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();

  }

 @GET
 @Path("/zipalertlist")
 @Produces(MediaType.APPLICATION_JSON)
 public Response zipAlertList() {
   String responseString = "{}";
   try {

     //get remote ip address from request
     String remoteIP = request.get().getRemoteAddr();
     //get the timestamp of the request
     long access_ts = System.currentTimeMillis();
     System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

     // String[] zip_list = new String[] { "40351","40513","40506" };
     Set<String> alertedZipCodes = Launcher.dbEngine.getAlertedZipCodes();


     Map<String, Object> responseMap = new HashMap<>();
     responseMap.put("ziplist", alertedZipCodes);

     responseString = gson.toJson(responseMap);

   } catch (Exception ex) {

       StringWriter sw = new StringWriter();
       ex.printStackTrace(new PrintWriter(sw));
       String exceptionAsString = sw.toString();
       ex.printStackTrace();

       return Response.status(500).entity(exceptionAsString).build();
   }
   return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
 }

 @GET
 @Path("/alertlist")
 @Produces(MediaType.APPLICATION_JSON)
 public Response alertList() {
   String responseString = "{}";
   try {

     //get remote ip address from request
     String remoteIP = request.get().getRemoteAddr();
     //get the timestamp of the request
     long access_ts = System.currentTimeMillis();
     System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

     // String[] zip_list = new String[] { "40351","40513","40506" };
     Set<String> alertedZipCodes = Launcher.dbEngine.getAlertedZipCodes();

     Map<String, Object> responseMap = new HashMap<>();
     if (alertedZipCodes.size() >= 5) {
       responseMap.put("state_status", "1");
     } else {
       responseMap.put("state_status", "0");
     }
     responseString = gson.toJson(responseMap);

   } catch (Exception ex) {

       StringWriter sw = new StringWriter();
       ex.printStackTrace(new PrintWriter(sw));
       String exceptionAsString = sw.toString();
       ex.printStackTrace();

       return Response.status(500).entity(exceptionAsString).build();
   }
   return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
 }

  @GET
  @Path("/testcount")
  @Produces(MediaType.APPLICATION_JSON)
  public Response diagnosisCounter() {
    String responseString = "{}";
    try {

      //get remote ip address from request
      String remoteIP = request.get().getRemoteAddr();
      //get the timestamp of the request
      long access_ts = System.currentTimeMillis();
      System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

      Map<String,String> responseMap = new HashMap<>();
      if(Launcher.cepEngine != null) {
              responseMap.put("positive_test", NUM_OF_POSITIVES.toString());
              responseMap.put("negative_test", NUM_OF_NEGATIVES.toString());
      } else {
        responseMap.put("positive_test", "234234");
        responseMap.put("negative_test", "234234");
      }

      responseString = gson.toJson(responseMap);

    } catch (Exception ex) {

        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        String exceptionAsString = sw.toString();
        ex.printStackTrace();

        return Response.status(500).entity(exceptionAsString).build();
    }
    return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
  }

  public static void addToPositiveCount(Integer amountToAdd) {
    NUM_OF_POSITIVES += amountToAdd;
  }

  public static void addToNegativeCount(Integer amountToAdd) {
    NUM_OF_NEGATIVES += amountToAdd;
  }

  @GET
  @Path("/getpatient/{mrn}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPatient( @PathParam("mrn") String mrn) {
    String responseString = "{}";
    try {

      //get remote ip address from request
      String remoteIP = request.get().getRemoteAddr();
      //get the timestamp of the request
      long access_ts = System.currentTimeMillis();
      System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);


      boolean wasOpen = Launcher.dbManager.isOpen;
      Launcher.dbManager.openDatabase();
      String location_id = Launcher.dbManager.getPatientLocation(mrn);
      if(!wasOpen)
        Launcher.dbManager.closeDatabase();

      Map<String,String> responseMap = new HashMap<>();
      if(Launcher.cepEngine != null) {
          responseMap.put("mrn", mrn);
          responseMap.put("location_code", location_id);
      } else {
          responseMap.put("mrn", mrn);
          responseMap.put("location_code", "-1");
      }

      responseString = gson.toJson(responseMap);

    } catch (Exception ex) {

        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        String exceptionAsString = sw.toString();
        ex.printStackTrace();

        return Response.status(500).entity(exceptionAsString).build();
    }
    return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
  }

  @GET
  @Path("/gethospital/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getHospital(@PathParam("id") int id) {
    String responseString = "{}";
    try {

      //get remote ip address from request
      String remoteIP = request.get().getRemoteAddr();
      //get the timestamp of the request
      long access_ts = System.currentTimeMillis();
      System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);
      boolean wasOpen = Launcher.dbManager.isOpen;
      Launcher.dbManager.openDatabase();
      int hospital[] = Launcher.dbManager.getHospitalInfo(id);
      if(!wasOpen)
        Launcher.dbManager.closeDatabase();

      Map<String,String> responseMap = new HashMap<>();
      if(Launcher.cepEngine != null) {
          responseMap.put("zipcode", ""+hospital[2]);
          responseMap.put("avalable_beds", ""+hospital[1]);
          responseMap.put("total_beds", ""+hospital[0]);

      } else {
        responseMap.put("total_beds", "-1");
        responseMap.put("avalable_beds", "-1");
        responseMap.put("zipcode", "-1");
      }

      responseString = gson.toJson(responseMap);

    } catch (Exception ex) {

        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        String exceptionAsString = sw.toString();
        ex.printStackTrace();

        return Response.status(500).entity(exceptionAsString).build();
    }
    return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
  }
}
