public class KafkaDemoMain {
    public static void main(String[] args) {

        // Ingest some sample data both to Customer and Balance topics
        KafkaDemoProducer.IngestDataToCustomerTopic();
        KafkaDemoProducer.IngestDataToBalanceTopic();

        // Wait for sometime
        try {
            Thread.sleep(35000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Join both customer and balance topics and sink the data to CustomerBalance
        KafkaDemoConsumer.JoinCustomerAndBalanceTopics();
    }
}
