package de.sec.dns.util;

/**
 * Source: http://jroller.com/tackline/entry/fast_immutables
 * 
 * Note we never hold on to more Long objects than the array size (threads
 * permitting), so we do not leak. The initial memory allocation is really quite
 * small in the scheme of things (around 16-32K as presented), so start up time
 * is insignificantly harmed. Worst case scenario is creating different values.
 * Always hitting the same index is bad. In those unusual cases the cache is
 * around half the speed of using Long.valueOf, in microbenchmarks on my
 * machine. In the best cases performance doubles. In the real world poor
 * locality and a turnover of ageing population may slow applications down (not
 * sure how an old array changes things). On the other hand, less thrashing of
 * the eden space should speed things up.
 * 
 * @author dh
 * 
 */

public abstract class Primitives {
	private static class DoubleCache {
		static final Double[] cache;
		// I would normally just use % cache.length,
		// but & constant is several cycles faster. :(
		static final int log2size = 20;
		static {
			final Double[] newCache = new Double[1 << log2size];
			// Eliminate the (explicit) null check,
			// by initialising with a single duff value.
			// We don't use Long.valueOf(128),
			// because that cache may never be needed. Possibly.
			// Don't use a value in the range [-128, 127]
			// as that may be expected interned.
			java.util.Arrays.fill(newCache, new Double(128));
			cache = newCache;
		}
	}

	private static class IntCache {
		static final Integer[] cache;
		// I would normally just use % cache.length,
		// but & constant is several cycles faster. :(
		static final int log2size = 20;
		static {
			final Integer[] newCache = new Integer[1 << log2size];
			// Eliminate the (explicit) null check,
			// by initialising with a single duff value.
			// We don't use Long.valueOf(128),
			// because that cache may never be needed. Possibly.
			// Don't use a value in the range [-128, 127]
			// as that may be expected interned.
			java.util.Arrays.fill(newCache, new Integer(128));
			cache = newCache;
		}
	}

	public static Double doubleValueOf(final double value) {
		// Double implementation in JDK doesn't cache at all
		// if (-128 <= value && value <= 127) {
		// Long itself has this range under control, in Sun's implementation.
		// Fast for these values, not so good if rarely used.
		// return Double.valueOf(value);
		// }
		// Very basic hash function, folds value once.
		final int index = (((int) Double.doubleToLongBits(value)) ^ (((int) Double
				.doubleToLongBits(value)) >> DoubleCache.log2size))
				& ((1 << DoubleCache.log2size) - 1);
		// If we were *really* worried about
		// cache misses from the value we are about to evict,
		// we could introduce a primitive array of hash hints
		// (e.g. bits 12 to 19 of the value).
		final Double candidate = DoubleCache.cache[index];
		if (candidate.doubleValue() == value) {
			return candidate;
		}
		final Double newValue = Double.valueOf(value);
		DoubleCache.cache[index] = newValue;
		return newValue;
	}

	public static Integer intValueOf(final int value) {
		if ((-128 <= value) && (value <= 127)) {
			// Long itself has this range under control, in Sun's
			// implementation.
			// Fast for these values, not so good if rarely used.
			return Integer.valueOf(value);
		}
		// Very basic hash function, folds value once.
		final int index = ((value) ^ ((value) >> IntCache.log2size))
				& ((1 << IntCache.log2size) - 1);
		// If we were *really* worried about
		// cache misses from the value we are about to evict,
		// we could introduce a primitive array of hash hints
		// (e.g. bits 12 to 19 of the value).
		final Integer candidate = IntCache.cache[index];
		if (candidate.intValue() == value) {
			return candidate;
		}
		final Integer newValue = Integer.valueOf(value);
		IntCache.cache[index] = newValue;
		return newValue;
	}

	public static void main(String[] args) {
		System.out.println("size in bit: " + DoubleCache.log2size);
		for (int value = 0; value < 5000; value++) {
			final int index = ((value * 100000) ^ (((value * 100000)) >> DoubleCache.log2size))
					& ((1 << DoubleCache.log2size) - 1);
			System.out.println(index);
		}
	}

	private Primitives() {
		throw new Error();
	}
}