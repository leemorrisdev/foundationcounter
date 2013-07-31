package uk.co.leemorris.foundation.counter;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;

import com.foundationdb.AsyncFuture;
import com.foundationdb.AsyncRetryable;
import com.foundationdb.Database;
import com.foundationdb.KeyValue;
import com.foundationdb.Nothing;
import com.foundationdb.RangeQuery;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;

/**
 * High contention counter.
 * Represents an integer value which can be incremented without conflict.
 * 
 * Uses a sharded representation (which scales with contention) along
 * with background coalescing.
 * @author lmorris
 *
 */
public class Counter {

	private Random randomGenerator = new Random();
	private Tuple prefix;
	
	private Database db;
	
	public Counter(Database db, Tuple prefix) {
		this.db = db;
		this.prefix = prefix;
	}
	
	/**
	 * Clear the keys for this counter
	 */
	public void clear() {
		Transaction tr = db.createTransaction();
		
		tr.clear(prefix.range().begin, prefix.range().end);

		tr.commit().get();
	}
	
	/**
	 * Merge keys to limit number of counter shards
	 * @param numReads
	 */
	protected void coalesce(int numReads) {

		int total = 0;
		Transaction tr = db.createTransaction();
		
		Tuple randPrefix = Tuple.fromBytes(prefix.pack())
				.addObject(randomGenerator.nextInt());
		
		RangeQuery range;
		if(randomGenerator.nextDouble() < 0.5) {
			range = tr.snapshot.getRange(randPrefix.pack(), prefix.range().end).limit(numReads);
		} else {
			range = tr.snapshot.getRange(prefix.range().begin, randPrefix.pack()).reverse().limit(numReads);
		}
		
		Iterator<KeyValue> iterator = range.iterator();
		
		while(iterator.hasNext()) {
			KeyValue kv = iterator.next();
			
			total += decodeInt(kv.getValue());
			
			tr.get(kv.getKey());
			tr.clear(kv.getKey());
		}
		
		Tuple p = Tuple.fromBytes(prefix.pack());
		p = p.addObject(randomGenerator.nextInt());
		tr.set(p.pack(), encodeInt(total));
		
		tr.commit();
	}
	
	/**
	 * Get the value of the counter with snapshot isolation (no
	 * transaction conflicts)
	 * @param tr
	 * @return
	 */
	protected int getSnapshot() {
		int total = 0;
		RangeQuery query = db.createTransaction().snapshot.getRange(prefix.range().begin, prefix.range().end);
		
		Iterator<KeyValue> iterator = query.iterator();
		
		while(iterator.hasNext()) {
			KeyValue kv = iterator.next();
			total += decodeInt(kv.getValue());
		}
		return total;
	}

	/**
	 * Add value to counter
	 * @param tr
	 * @param value
	 */
	public void add(final Integer value) {

		final Transaction tr = db.createTransaction();

		Integer nextInt = randomGenerator.nextInt();
		
		Tuple randPrefix = Tuple.fromBytes(prefix.pack());
		randPrefix = randPrefix.addObject(nextInt);

		tr.set(randPrefix.pack(), encodeInt(value));

		if(randomGenerator.nextDouble() < 0.1) {
			coalesce(20);
		}
		
		db.run(new AsyncRetryable() {

			public AsyncFuture<Nothing> attempt(Transaction arg0) {
				return tr.commit();
			}
			
		});
	}
	
	/**
	 * Create byte array from Integer
	 * @param value
	 * @return
	 */
	private byte[] encodeInt(Integer value) {
		byte[] output = new byte[4];
		ByteBuffer.wrap(output).putInt(value);
		
		return output;
	}
	
	/**
	 * Create Integer from byte array
	 * @param value
	 * @return
	 */
	private Integer decodeInt(byte[] value) {
		return ByteBuffer.wrap(value).getInt();
	}
}