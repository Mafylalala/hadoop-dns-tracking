package de.sec.dns.playground;

public class HugeArraysTest {

	/**
	 * @param args
	 */

	private static Runtime rt = Runtime.getRuntime();

	public static long getMemoryInfo() {

		long fm = rt.freeMemory();
		long tm = rt.totalMemory();
		long mm = rt.maxMemory();
		System.err.println("   Free memory = " + fm);
		System.err.println("   Total memory = " + tm);
		System.err.println("   Maximum memory = " + mm);
		System.err.println("   Memory used = " + (tm - fm));
		return tm - fm;
	}

	public static void main(String[] args) {

		int size = 16 * 1000 * 1000;

		System.out.println("Allocating array of " + size + " strings...");

		HugeArraysTest.getMemoryInfo();

		String[] strings = new String[size];
		System.out.println("Done.");
		HugeArraysTest.getMemoryInfo();

		System.out.println("Filling array of strings...");
		for (int i = 0; i < size; i++) {
			strings[i] = "1234567890123456789012345645";

			if (i % 2000000 == 0) {
				System.out.println("Counter: " + i);
			}
		}
		System.out.println("Done.");
		HugeArraysTest.getMemoryInfo();

		/*
		 * System.out.println("Sorting strings...");
		 * java.util.Arrays.sort(strings);
		 * 
		 * System.out.println("Allocating array of "+ size +" int's...");
		 * 
		 * int[] ints = new int[size]; System.out.println("Done.");
		 * HugeArraysTest.getMemoryInfo();
		 * 
		 * System.out.println("Filling array of ints..."); for(int
		 * i=0;i<size;i++) { ints[i] = i; if(i%2000000==0)
		 * System.out.println("Counter: "+i); } System.out.println("Done.");
		 * HugeArraysTest.getMemoryInfo();
		 * 
		 * System.out.println("Searching for "+ size +" random strings...");
		 * long start = System.currentTimeMillis(); Random r= new Random();
		 * 
		 * for(int i=0;i<size;i++) { int index =
		 * java.util.Arrays.binarySearch(strings, "String "+r.nextInt(size));
		 * if(i%(size/10)==0) { System.out.println("Counter: "+i); if(index<0)
		 * System.out.println("not found at index: "+index); // should not occur
		 * for us! else System.out.println("found at index: "+index); } } long
		 * difference = System.currentTimeMillis() - start;
		 * System.out.println("Search duration (msec): "+difference);
		 */
	}

}
