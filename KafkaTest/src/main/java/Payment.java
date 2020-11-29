import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

public class Payment implements Runnable {

	private String accountNr;
	private String source;
	private double amountSpent;

	// source should be either SWISH or CREDIT
	// assume that accountNr is a number between 1000 to 2000.
	Payment(String accountNr, String source) {
		this.accountNr = accountNr;
		this.source = source;
	}

	@Override
	public void run() {
		Properties propsProducer = new Properties();
		propsProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				org.apache.kafka.common.serialization.StringSerializer.class);
		propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		propsProducer.put("schema.registry.url", "http://localhost:8081");
		KafkaProducer producer = new KafkaProducer(propsProducer);

		String key = "money-spent";
		String userSchema = "{\"type\":\"record\"," + "\"name\":\"myrecord\","
				+ "\"fields\":[{\"name\":\"accountNr\",\"type\":\"string\"}, "
				+ "{\"name\":\"amountSpent\",\"type\":\"double\"}, " + "{\"name\":\"source\",\"type\":\"string\"}]}";

		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(userSchema);

		boolean running = true;
		while (running) {

			GenericRecord avroRecord = new GenericData.Record(schema);

			// accountNr = String.valueOf((int)(Math.random()*(2000-1000+1)+1000));
			avroRecord.put("accountNr", accountNr);
			// assume that amount spent is some number between 10 to 1000;
			amountSpent = Math.random() * (1000 - 10 + 1) + 10;
			avroRecord.put("amountSpent", amountSpent);

			avroRecord.put("source", source);

			final String topic = "seb-account";

			ProducerRecord<Object, Object> recordProducer = new ProducerRecord<>(topic, key, avroRecord);
			if (getBalance(accountNr) < amountSpent) {
				System.out.printf("Amount is %, .2f \n", amountSpent);
				System.out.printf("Balance is not enough: %, .2f \n", getBalance(accountNr));
				break;
			} else {
				try {
					producer.send(recordProducer);
				} catch (SerializationException e) {
					e.printStackTrace();
				}

			}

			// Sleep 5 seconds
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				running = false;
				e.printStackTrace();
			}
		}
		producer.flush();
		producer.close();

	}

	double getBalance(String accountNr) {
		return BalanceController.getBalance(accountNr);
	}

}
