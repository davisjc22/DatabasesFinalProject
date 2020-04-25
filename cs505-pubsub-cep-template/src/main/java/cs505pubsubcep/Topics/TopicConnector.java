package cs505pubsubcep.Topics;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import cs505pubsubcep.Launcher;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;


public class TopicConnector {

    private Gson gson;
    final Type typeOf = new TypeToken<List<Map<String,String>>>(){}.getType();

    private String EXCHANGE_NAME = "patient_data";

    public TopicConnector() {
        gson = new Gson();
    }

    public void connect() {

        try {

            String hostname = "";
            String username = "";
            String password = "";
            String virtualhost = "";

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(hostname);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setVirtualHost(virtualhost);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, EXCHANGE_NAME, "#");


            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received Batch'" +
                        delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

                List<Map<String,String>> incomingList = gson.fromJson(message, typeOf);
                for(Map<String,String> map : incomingList) {
                    System.out.println("INPUT CEP EVENT: " +  map);
                    Launcher.cepEngine.input(Launcher.inputStreamName, gson.toJson(map));
                }
                System.out.println("");
                System.out.println("");

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
}

}
