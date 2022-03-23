import com.threerivers.kafkademo.Balance;
import com.threerivers.kafkademo.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;

public class KafkaDemoProducer {
    public static void IngestDataToCustomerTopic() {
        Logger logger = LoggerFactory.getLogger(KafkaDemoProducer.class.getName());

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, Customer> producer = new KafkaProducer<String, Customer>(properties);

        try {
            Scanner scanner = new Scanner(new File("/home/vineel/data/customers.txt"));
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String []fields = line.split(",");

                if (fields.length < 4)
                    continue;

                Customer customer = Customer.newBuilder()
                                            .setCustomerId(fields[0])
                                            .setName(fields[1])
                                            .setPhoneNumber(fields[2])
                                            .setAccountId(fields[3]).build();

                ProducerRecord<String, Customer> record = new ProducerRecord<String, Customer>("Customer", customer);
                producer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        System.out.println("Sent message successfully" + recordMetadata);
                    } else {
                        System.out.println("Sending failed" + e);
                    }
                });
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        producer.flush();
        producer.close();
    }

    public static void IngestDataToBalanceTopic() {
        Logger logger = LoggerFactory.getLogger(KafkaDemoProducer.class.getName());

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, Balance> producer = new KafkaProducer<String, Balance>(properties);

        try {
            Scanner scanner = new Scanner(new File("/home/vineel/data/balances.txt"));
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String []fields = line.split(",");

                if (fields.length < 3)
                    continue;

                Balance balance = Balance.newBuilder()
                                        .setBalanceId(fields[0])
                                        .setAccountId(fields[1])
                                        .setBalance(Double.parseDouble(fields[2])).build();

                ProducerRecord<String, Balance> record = new ProducerRecord<String, Balance>("Balance", balance);
                producer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        System.out.println("Sent message successfully" + recordMetadata);
                    } else {
                        System.out.println("Sending failed" + e);
                    }
                });
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        producer.flush();
        producer.close();
    }

}
