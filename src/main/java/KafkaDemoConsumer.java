import com.threerivers.kafkademo.Balance;
import com.threerivers.kafkademo.Customer;
import com.threerivers.kafkademo.CustomerBalance;

import model.CustomerBalanceJoiner;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KafkaDemoConsumer {
    public static void JoinCustomerAndBalanceTopics() {
        Logger logger = LoggerFactory.getLogger(KafkaDemoConsumer.class.getName());

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-stream-inner-join");
        properties.put("schema.registry.url", "http://localhost:8081");

        StreamsBuilder builder = new StreamsBuilder();

        // Create required serializer/deserializer using Avro
        Serde<Customer> customerAvroSerde = new SpecificAvroSerde<>();
        Serde<Balance> balanceAvroSerde = new SpecificAvroSerde<>();
        Serde<String> keySerde = Serdes.String();

        // Fetch the data from both the topics
        KStream<String, Customer> customerStream = builder.stream("Customer", Consumed.with(keySerde, customerAvroSerde));
        KStream<String, Balance> balanceStream = builder.stream("Balance", Consumed.with(keySerde, balanceAvroSerde));

//        customerStream.print(Printed.toSysOut());
//        balanceStream.print(Printed.toSysOut());

        // Use a custom value joiner to join the stream
        ValueJoiner<Customer, Balance, CustomerBalance> customerBalanceJoiner = new CustomerBalanceJoiner();
        KStream<String, CustomerBalance> customerBalanceStream = customerStream.join(balanceStream,
                                                                                     customerBalanceJoiner,
                                                                                     JoinWindows.of(Duration.ofMinutes(20)));

        // Sink data to CustomerBalance topic
        Serde<CustomerBalance> customerBalanceAvroSerde = new SpecificAvroSerde<>();
        customerBalanceStream.to("CustomerBalance", Produced.with(keySerde, customerBalanceAvroSerde));

        StreamsConfig streamsConfig = new StreamsConfig(properties);
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.start();
        try {
            Thread.sleep(35000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        kafkaStreams.close();
    }
}
