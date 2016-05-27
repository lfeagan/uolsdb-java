
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.MessageFormat;
import java.util.List;

import org.testng.annotations.Test;

import v1.DatabaseGenerator;
import v1.Entry;
import v1.SecureLog;

public class MyTest {

	public static void main(String[] args) throws IOException {
		genDb();
//		foo();
	}
	
	@Test
	public static void genDb() throws IOException {
		SecureLog db = new SecureLog("/tmp/uolsdb_0");
		DatabaseGenerator dbgen = new DatabaseGenerator();
		int numberOfEntries = 100000;
		System.out.println("starting to write");
		final long startTime = System.currentTimeMillis();
		for (int i=0; i < numberOfEntries; ++i) {
			final Entry entry;
			if (i == 0) {
				entry = dbgen.randomEntry(db, 0);
			} else {
				entry = dbgen.randomEntry(db, 1);
			}
			entry.write();
		}
		final long endTime = System.currentTimeMillis();
		final double elapsed = (endTime - startTime) / 1000.0;
		final double rate = numberOfEntries / elapsed;
		System.out.println(MessageFormat.format("insert elapsed: {0} , rate: {1}", elapsed, rate));
		queryLatest(db);
		queryLineage(db);
		db.close();
	}
	
	public static void queryLatest(SecureLog db) throws IOException {
		final long startTime = System.currentTimeMillis();
		int size = db.size().intValue();
		for (int i=0; i < size; ++i) {
			db.getEntryOffset(i);
		}
		final long endTime = System.currentTimeMillis();
		final double elapsed = (endTime - startTime) / 1000.0;
		final double rate = size / elapsed;
		System.out.println(MessageFormat.format("query elapsed: {0} , rate: {1}", elapsed, rate));
	}
	
	public static void queryLineage(SecureLog db) throws IOException {
		final long startTime = System.currentTimeMillis();
		long size = db.size();
		long scanned = 0;
		for (int i=0; i < size; ++i) {
			List<Long> lineage = db.getLineage(size - 1L - i);
			scanned += lineage.size();
			//System.out.println("lineage: " + lineage);
		}
		final long endTime = System.currentTimeMillis();
		final double elapsed = (endTime - startTime) / 1000.0;
		final double rate = size / elapsed;
		System.out.println(MessageFormat.format("lineage elapsed: {0} , rate: {1} , scanned: {2}", elapsed, rate, scanned));		
	}
	
	public static void queryPointInTime(SecureLog db) {
		
	}
	
	@Test
	public static void foo() throws IOException {
		File file = new File("/tmp/uolsdb_0");
		if (!file.exists()) {
			file.createNewFile();
		}
		FileOutputStream fos = new FileOutputStream(file);
		FileChannel fc = fos.getChannel();
		//FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(1234);
		bb.flip();
		while (bb.hasRemaining()) {
			fc.write(bb);
		}
		fos.flush();
//		fos.close();
		fc.force(true);
		fc.close();
	}
	
}
