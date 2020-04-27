package cs505pubsubcep.CEP;

import io.siddhi.core.util.transport.InMemoryBroker;
import cs505pubsubcep.httpcontrollers.API;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Set;
import cs505pubsubcep.Launcher;
import java.util.HashMap;
import java.util.Map;


public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    private static final int BEGINNING_MMD_INDEX = 12;
    private static final int ENDING_MMD_INDEX = 48;
    private static final int BEGINNING_ZIP_INDEX = 59;
    private static final int ENDING_ZIP_INDEX = 64;
    private static final int BEGINNING_STATUS_INDEX = 86;
    private static final int ENDING_STATUS_INDEX = 87;


    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
          // TODO: COMMENT THIS BACK
            // System.out.println("OUTPUT CEP EVENT: " + msg);
            // System.out.println("");
            // convert message to array using GSON
            // System.out.println(msg);
            System.out.println(Launcher.dbEngine.getZipCodes());
            Map<String,Integer> oldValues = Launcher.dbEngine.getZipCodes();
            Map<String,Integer> newValues = new HashMap<>();
            newValues.putAll(oldValues);
            ArrayList<?> jsonArray = new Gson().fromJson(msg.toString(), ArrayList.class);
            // System.out.println(jsonArray);
            System.out.println(jsonArray.size());
            for (int i = 0; i < jsonArray.size(); i++) {
              String MMD = "";
              String Zip = "";
              String Status = "";
              // ArrayList<?> eventArray = new Gson().fromJson(jsonArray.get(i).toString(), ArrayList.class);
              // for (int j = 0; j < eventArray.size(); j++) {
              //   System.out.println(eventArray.get(i));
              // }
              // String duplicate = "{event={mrn=f99a64d3-393f-4d79-a819-169e8e55a1fc, zip_code=41096, patient_status_code=6}}";
              System.out.println(jsonArray.get(i).toString());
              char[] ch = jsonArray.get(i).toString().toCharArray(); // this line of code here is truly awful.
              // char[] ch = duplicate.toCharArray();
              for (int j = BEGINNING_MMD_INDEX; j < ENDING_MMD_INDEX; j++) {
                  MMD += ch[j];
              }
              for (int j = BEGINNING_ZIP_INDEX; j < ENDING_ZIP_INDEX; j++) {
                  Zip += ch[j];
              }
              for (int j = BEGINNING_STATUS_INDEX; j < ENDING_STATUS_INDEX; j++) {
                  Status += ch[j];
              }
              System.out.println(MMD);
              System.out.println(Zip);
              System.out.println(Status);

              if (Status.equals("1") || Status.equals("4")) {
                // if the patient tested negative
                System.out.println("Negative");
                API.NUM_OF_NEGATIVES++;
              } else if (Status.equals("2") || Status.equals("5") || Status.equals("6")) {
                // if the patient tested positive
                System.out.println("Positive");
                API.NUM_OF_POSITIVES++;
                if(Launcher.dbEngine.zipCodeExists(Zip)) {
                  // if it already exists, update it's total number of positive cases
                  newValues.replace(Zip, newValues.get(Zip) + 1);
                  System.out.println("trying to replace");
                } else {
                  // else it is the first time positive cases have been seen for this zip code, insert them new
                  System.out.println("trying to insert");
                  String insertQuery = "INSERT INTO zipcodes (zip_code, positives, alert)"
                  + String.format(" values(%1$s, %2$d, %3$d)", Zip, 1, 0);
                  Launcher.dbEngine.executeUpdate(insertQuery);
                  oldValues.put(Zip, 0);
                  newValues.put(Zip, 1);
                }
                // Integer numOfnumbers = 3;
                // Integer bullshit = 0;
                // String insertQuery = "INSERT INTO zipcodes (zip_code, positives, alert)"
                // + String.format(" values(%1$s, %2$d, %3$d)", Zip, numOfnumbers, bullshit);
                // Launcher.dbEngine.executeUpdate(insertQuery);
              }

            }

            System.out.println(newValues);
            Set<String> seenZipCodes = newValues.keySet();
            System.out.println(seenZipCodes);
            for( String key : seenZipCodes) {
              // System.out.println(key);
              Integer alert = 0;
              if (newValues.get(key) >= 2 * oldValues.get(key) && oldValues.get(key) != 0) {
                alert = 1;
                System.out.println("they are super different");
              }
              // else if (newValues.get(key) > oldValues.get(key)) {
              //   System.out.println("they are DIFFERENT");
              //   String updateQuery = "UPDATE zipcodes"
              //   + String.format(" SET positives = %1$d, alert = %2$d", newValues.get(key), alert)
              //   + String.format(" WHERE zip_code = %1$s", key);
              //   Launcher.dbEngine.executeUpdate(updateQuery);
              // } else {
              //   // System.out.println("they are the same");
              // }
              String updateQuery = "UPDATE zipcodes"
              + String.format(" SET positives = %1$d, alert = %2$d", newValues.get(key), alert)
              + String.format(" WHERE zip_code = %1$s", key);
              Launcher.dbEngine.executeUpdate(updateQuery);
            }



            // String[] sstr = String.valueOf(msg).split(":");
            // String[] outval = sstr[2].split("}");
            // System.out.println(sstr);
            // System.out.println(outval);

            //Launcher.accessCount = Long.parseLong(outval[0]);

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
