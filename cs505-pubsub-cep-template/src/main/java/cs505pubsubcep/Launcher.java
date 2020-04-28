package cs505pubsubcep;

import cs505pubsubcep.CEP.CEPEngine;
import cs505pubsubcep.Topics.TopicConnector;
import cs505pubsubcep.httpfilters.AuthenticationFilter;

import cs505pubsubcep.database.DBEngine;
import cs505pubsubcep.database.GraphDBManager;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;


public class Launcher {

    public static final String API_SERVICE_KEY = "12110132"; //Change this to your student id
    public static final int WEB_PORT = 8088;
    public static String inputStreamName = null;
    public static long accessCount = -1;

    public static TopicConnector topicConnector;

    public static CEPEngine cepEngine = null;
    public static DBEngine dbEngine;
    public static GraphDBManager dbManager;

    public static void main(String[] args) throws IOException {

        //Graph database initialization
        dbManager = new GraphDBManager();
        System.out.println("Graph Database Started...");
        
        System.out.println("Starting CEP...");
        //Embedded database initialization

        cepEngine = new CEPEngine();
        

        //START MODIFY
        inputStreamName = "PatientInStream";
        String inputStreamAttributesString = "first_name string, last_name string, mrn string, zip_code string, patient_status_code string";

        String outputStreamName = "PatientOutStream";
        String outputStreamAttributesString = "mrn string, zip_code string, patient_status_code string";


        String queryString = " " +
                "from PatientInStream#window.timeBatch(15 sec) " +
                "select mrn, zip_code, patient_status_code " +
                "group by zip_code " +
                "insert into PatientOutStream; ";

        //END MODIFY

        cepEngine.createCEP(inputStreamName, outputStreamName, inputStreamAttributesString, outputStreamAttributesString, queryString);

        System.out.println("CEP Started...");


        //starting Collector
        topicConnector = new TopicConnector();
        topicConnector.connect();

        System.out.println("Starting Embedded Database...");
        //Embedded database initialization
        dbEngine = new DBEngine();
        System.out.println("Embedded Database Started...");

        //Embedded HTTP initialization
        startServer();


        try {
            while (true) {
                Thread.sleep(5000);
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void startServer() throws IOException {

        final ResourceConfig rc = new ResourceConfig()
        .packages("cs505pubsubcep.httpcontrollers")
        .register(AuthenticationFilter.class);

        System.out.println("Starting Web Server...");
        URI BASE_URI = UriBuilder.fromUri("http://0.0.0.0/").port(WEB_PORT).build();
        HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, rc);

        try {
            httpServer.start();
            System.out.println("Web Server Started...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
