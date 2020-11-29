import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class BalanceController {
	// private static double balance = 10000.00;

	private static HashMap<String, Double> balanceMap;

	// assume that every account(1000--2000) has a budget of 10000.
	static {
		balanceMap = new HashMap<String, Double>();
		for (int account = 1000; account <= 2000; account++) {
			balanceMap.put(String.valueOf(account), 10000.00);
		}
	}

	public synchronized static double getBalance(String accountNr) {
		return balanceMap.get(accountNr);
	}

	private synchronized static void setBalance(String accountNr, double balance) {
		BalanceController.balanceMap.put(accountNr, balance);
	}

	public void caculateBalance() {
		Properties propsConsumer = new Properties();
		propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, "seb-account");
		propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"io.confluent.kafka.serializers.KafkaAvroDeserializer");
		propsConsumer.put("schema.registry.url", "http://localhost:8081");
		propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		final String topic = "seb-account";

		KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(propsConsumer);
		consumer.subscribe(Arrays.asList(topic));
		try {
			while (true) {
				ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(3000));
				double totalCost = 0;
				for (ConsumerRecord<String, GenericRecord> record : records) {
					System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
							record.value());
					totalCost = totalCost + (double) record.value().get("amountSpent");
					String accNr = record.value().get("accountNr").toString();
					setBalance(accNr, getBalance(accNr) - totalCost);	
					System.out.printf("balance = %, .2f \n", balanceMap.get(accNr));
					System.out.println("======================================");
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
