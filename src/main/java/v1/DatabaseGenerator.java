package v1;


import java.util.Random;

public class DatabaseGenerator {
	
	public int numberOfEntries = 100;
	public int maxLinksPerEntry = 2;
	
	private Random random = new Random();
	
	public SecureLog generate() {
		final SecureLog db = new SecureLog("/tmp/uolsdb_0");
		for (long i=0; i < numberOfEntries; ++i) {
			Entry entry = db.createEntry();
			if (i > 0) {
				long linkSequenceNumber = Math.abs(random.nextLong()) % i;
				entry.addReferenceTo(db.getEntry(linkSequenceNumber), "foo");
			}
		}
		return db;
	}
	
	public Entry randomEntry(final SecureLog db, final int numberOfLinks) {
		final Entry entry = db.createEntry();
		int linkCount=0;
		while (linkCount < numberOfLinks) {
			long linkSequenceNumber = Math.abs(random.nextLong()) % (db.size()-1);
			entry.addReferenceTo(db.getEntry(linkSequenceNumber), "foo");
			++linkCount;
		}
		return entry;
	}
	
}
