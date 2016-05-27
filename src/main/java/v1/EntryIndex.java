package v1;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class EntryIndex {
	
	private final DB db;
	private final BTreeMap<Long, Long> map; 
	
	public EntryIndex() {
//		db = DBMaker.memoryDB().make();
		db = DBMaker.fileDB("/tmp/entryIndex.db").fileMmapEnable().make();
		map = db.treeMap("map")
		        .keySerializer(Serializer.LONG)
		        .valueSerializer(Serializer.LONG)
		        .createOrOpen();		
	}
	
	void addEntry(Entry entry) {
		map.put(entry.getSequenceNumber(), entry.getOffset());
	}
	
	public Long getOffset(final Long sequenceNumber) {
		final Long offset = map.get(sequenceNumber);
		return offset;
	}
	
	public Entry getEntry(long sequenceNumber) {
		Long offset = map.get(sequenceNumber);
		if (offset == null) {
			return null;
		}
//		Entry entry = 
		return null;
	}
	
	public void close() {
		db.close();
	}

}
