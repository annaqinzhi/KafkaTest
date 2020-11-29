
public class KafkaMain {
	public static void main(String[] args) {
		
		BalanceController bc = new BalanceController();

		Thread producer1000Swish = new Thread(new Payment("1000", "SWISH"));
		Thread producer1000Credit = new Thread(new Payment("1000","CREDIT"));
		
		Thread producer1234Swish = new Thread(new Payment("1234", "SWISH"));
		Thread producer1234Credit = new Thread(new Payment("1234","CREDIT"));

		producer1000Swish.start();
		producer1000Credit.start();
		producer1234Swish.start();
		producer1234Credit.start();
		bc.caculateBalance();

	}
}
