package model;

import com.threerivers.kafkademo.Balance;
import com.threerivers.kafkademo.Customer;
import com.threerivers.kafkademo.CustomerBalance;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class CustomerBalanceJoiner implements ValueJoiner<Customer, Balance, CustomerBalance> {

    @Override
    public CustomerBalance apply(Customer customer, Balance balance) {
        if (customer.getCustomerId() == balance.getAccountId()) {
            CustomerBalance customerBalance = new CustomerBalance();
            customerBalance.setAccountId(customer.getAccountId());
            customerBalance.setCustomerId(customer.getCustomerId());
            customerBalance.setPhoneNumber(customer.getPhoneNumber());
            customerBalance.setBalance(balance.getBalance());
            return customerBalance;
        } else {
            return null;
        }
    }
}
