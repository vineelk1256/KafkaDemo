package serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import model.BalanceTemp;

public class BalanceDeserializer implements Deserializer {
    @Override
    public Object deserialize(String s, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        BalanceTemp customer = null;
        try {
            customer = mapper.readValue(arg1, BalanceTemp.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return customer;
    }
}
