package uk.co.leemorris.foundation.counter;

import java.nio.ByteBuffer;
import java.util.Random;

import com.foundationdb.Database;
import com.foundationdb.KeyValue;
import com.foundationdb.ReadTransaction;
import com.foundationdb.Transaction;
import com.foundationdb.async.AsyncIterable;
import com.foundationdb.async.Function;
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
	 * Clear the keys for this counter. This is a blocking function, if this function
	 *  returns without throwing an exception, the database has been successfully cleared.
	 */
	public void clear() {
		db.run(new Function<Transaction, Void>() {
			@Override
			public Void apply(Transaction tr) {
				tr.clear(prefix.range());
				return null;
			}
		});
	}

	/**
	 * Merge keys to limit number of counter shards. This method is unlike most uses of
	 *  FoundationDB -- that is there will be a call to {@link Transaction#commit()} that
	 *  does not use a retry loop. In addition, since there is no concern about the return
	 *  value of the call to {@code commit()}, we will not block on the result.
	 *
	 * @param numReads the limit to the number of keys to coalesce
	 */
	protected void coalesce(int numReads) {
		final Transaction tr = db.createTransaction();
		int total = 0;

		Tuple randPrefix = prefix.add(randomGenerator.nextInt());

		AsyncIterable<KeyValue> values;
		ReadTransaction snapshot = tr.snapshot();
		if(randomGenerator.nextDouble() < 0.5) {
			values = snapshot.getRange(randPrefix.pack(), prefix.range().end, numReads);
		} else {
			values = snapshot.getRange(prefix.range().begin, randPrefix.pack(), numReads, true);
		}

		for(KeyValue kv : values) {
			total += decodeInt(kv.getValue());
			tr.get(kv.getKey());
			tr.clear(kv.getKey());
		}

		Tuple p = prefix.add(randomGenerator.nextInt());
		tr.set(p.pack(), encodeInt(total));

		// We need to make sure that tr does not go out of scope since this would
		//  garbage collect the transaction and cancel the commit. Otherwise, do
		//  not block -- just start the commit, register a callback (to hold onto
		//  tr) and return.
		tr.commit().onReady(new Runnable() {
			@Override
			public void run() {
				// This is NOT NEEDED other than to hold onto tr. Calling hashCode()
				//  is used only since it's a cheap operation!
				tr.hashCode();
			}
		});
	}

	/**
	 * Get the value of the counter with snapshot isolation (no transaction conflicts).
	 *  Uses {@link Database#run(Function)} to return a value, while still using all
	 *  built-in retry logic.
	 *
	 * @return the sum of all values in the database
	 */
	protected int getSnapshot() {
		return db.run(new Function<Transaction, Integer>() {
			@Override
			public Integer apply(Transaction tr) {
				int total = 0;
				for(KeyValue kv : tr.snapshot().getRange(prefix.range())) {
					total += decodeInt(kv.getValue());
				}
				return total;
			}
		});
	}

	/**
	 * Add value to counter. Every ten attempts to add to the counter a pass
	 *  will be made to coalesce values. This is a blocking function -- if this
	 *  function returns without throwing an exception, the value has been
	 *  persisted to the database.
	 *
	 * @param value the amount to add to the counter
	 */
	public void add(final Integer value) {
		Integer nextInt = randomGenerator.nextInt();
		final byte[] key = prefix.add(nextInt).pack();
		final byte[] valueBytes = encodeInt(value);

		db.run(new Function<Transaction, Void>() {
			@Override
			public Void apply(Transaction tr) {
				tr.set(key, valueBytes);
				return null;
			}

		});

		if(randomGenerator.nextDouble() < 0.1) {
			coalesce(20);
		}
	}

	/**
	 * Serialize an integer into a byte array. This will use big-endian encoding.
	 *
	 * @param value the number to encode
	 * @return a four byte array storing {@code value}
	 */
	private static byte[] encodeInt(int value) {
		byte[] output = new byte[4];
		ByteBuffer.wrap(output).putInt(value);

		return output;
	}

	/**
	 * Read an integer from byte array. The integer must be encoded in big-endian.
	 *
	 * @param value a four byte array containing an encoded integer
	 * @return
	 */
	private static int decodeInt(byte[] value) {
		if(value.length != 4) {
			throw new IllegalArgumentException("Array must be of size 4");
		}
		return ByteBuffer.wrap(value).getInt();
	}
}