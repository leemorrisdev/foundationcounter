package uk.co.leemorris.foundation.counter;

import static com.jayway.awaitility.Awaitility.await;

import java.util.concurrent.Callable;

import org.junit.Test;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.tuple.Tuple;

public class CounterTest {
	private final static int NUM_THREADS = 100;
	private final static int NUM_INCREMENTS = 100;

	private Counter c;

	@Test
	public void testCounter() throws Exception {
		FDB fdb = FDB.selectAPIVersion(100);
		Database db = fdb.open();

		byte[] counterName = "TESTCOUNTER".getBytes();
		Tuple t = new Tuple();
		t = t.add(counterName);
		c = new Counter(db, t);

		c.clear();

		long start = System.currentTimeMillis();
		for(int i = 0; i < NUM_THREADS; i++) {
			new Thread(new Runnable() {

				@Override
				public void run() {
					for(int idx = 0; idx < NUM_INCREMENTS; idx++) {
						c.add(1);
					}
				}
			}).start();
		}

		await().until(counterComplete());
		long end = System.currentTimeMillis();

		System.out.println("Total time " + (end - start));
	}

	private Callable<Boolean> counterComplete() {

		return new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				return c.getSnapshot() == 10000;
			}
		};
	}
}
