package v1;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class SecureLog {
	
	final String filePath;
	final AtomicLong SEQUENCE_NUMBER = new AtomicLong(0);	
	final List<Entry> entries = new ArrayList<Entry>();
	final EntryIndex entryIndex = new EntryIndex();
	
	public SecureLog(final String filePath) {
		this.filePath = filePath;
	}
	
	public void close() {
		entryIndex.close();
	}
	
	public Entry createEntry() {
		final Entry entry = new Entry(this, SEQUENCE_NUMBER.getAndIncrement());
		this.entries.add(entry);
		return entry;
	}
	
	public Long getEntryOffset(final long sequenceNumber) {
		return entryIndex.getOffset(sequenceNumber);
	}
	
	public Long size() {
		return SEQUENCE_NUMBER.get();
	}
	
	public Entry getEntry(final long sequenceNumber) {
		if (sequenceNumber > Integer.MAX_VALUE) {
			for (Entry e : entries) {
				if (e.getSequenceNumber() == sequenceNumber) {
					return e;
				}
			}
		} else {
			return entries.get((int) sequenceNumber);
		}
		return null;
	}
	
	public List<Long> getLineage(final Long sequenceNumber) throws IOException {
		final List<Long> history = new ArrayList<Long>();
		Long offset = entryIndex.getOffset(sequenceNumber);
		File file = new File(filePath);
		try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ);) {
			fc.position(offset);
			Queue<EntryReference> pending = new LinkedList<EntryReference>();
			pending.addAll(readEntryReferencesFromDisk(fc));
			while(!pending.isEmpty()) {
				EntryReference er = pending.poll();
				history.add(er.getSequenceNumber());
//				System.out.println(er.getOffset());
				fc.position(er.getOffset());
				pending.addAll(readEntryReferencesFromDisk(fc));
			}
		}
		return history;
	}
	
	public FileChannel getWritableFileChannel() throws IOException {
		File file = new File(filePath);
		if (!file.exists()) {
			file.createNewFile();
		}
		//FileOutputStream fos = new FileOutputStream(file);
		//FileChannel fc = fos.getChannel();
		FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
		return fc;
	}
		
	public void writeTo(final String filePath) throws IOException {
		FileChannel fc = getWritableFileChannel();
		for (Entry entry : entries) {
			entry.writeTo(fc);
			entryIndex.addEntry(entry);
			//System.out.println(entry);
		}
//		fos.flush();
		//fos.close();
		fc.force(true);
		fc.close();
	}
	
	public static SecureLog openDatabase(final String filePath) throws IOException {
		File file = new File(filePath);
		FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
		final SecureLog db = new SecureLog(filePath);
		Entry entry = null;
		boolean keepReading = true;
		while (keepReading) {
			try {
				entry = db.readEntryFromDisk(fc);
				db.entries.add(entry);
			} catch (IOException e) {
				keepReading = false;
			}
		}
		return db;
	}
	
	private Entry readEntryFromDisk(FileChannel fileChannel) throws IOException {
		final long initialPosition = fileChannel.position();
		ByteBuffer bb = ByteBuffer.allocate(16);
		int bytesRead = fileChannel.read(bb);
		final int length = bb.getInt();
		long sequenceNumber = bb.getLong();
		int numberOfEntryReferences = bb.getInt();
		List<EntryReference> entryReferences = new LinkedList<EntryReference>();
		for (int i=0; i < numberOfEntryReferences; ++i) {
			entryReferences.add(readEntryReferenceFromDisk(fileChannel));
		}
		assert(initialPosition + length == fileChannel.position());
		final Entry entry = new Entry(this, sequenceNumber, entryReferences);
		return entry;
	}
	
	public static Entry readEntry(SecureLog secureLog, ByteBuffer byteBuffer) {
		final int length = byteBuffer.getInt();
		long sequenceNumber = byteBuffer.getLong();
		int numberOfEntryReferences = byteBuffer.getInt();
		List<EntryReference> entryReferences = new LinkedList<EntryReference>();
		for (int i=0; i < numberOfEntryReferences; ++i) {
			entryReferences.add(readEntryReference(byteBuffer));
		}
		final Entry entry = new Entry(secureLog, sequenceNumber, entryReferences);
		return entry;
	}
	
	private List<EntryReference> readEntryReferencesFromDisk(FileChannel fileChannel) throws IOException {
		final long initialPosition = fileChannel.position();
		ByteBuffer bb = ByteBuffer.allocate(16);
		int bytesRead = fileChannel.read(bb);
//		System.out.println(bytesRead);
		bb.flip();
		final int length = bb.getInt();
		long sequenceNumber = bb.getLong();
		int numberOfEntryReferences = bb.getInt();
		List<EntryReference> entryReferences = new LinkedList<EntryReference>();
		for (int i=0; i < numberOfEntryReferences; ++i) {
			final EntryReference er = readEntryReferenceFromDisk(fileChannel);
			//System.out.println(er);
			entryReferences.add(er);
		}
		assert(initialPosition + length == fileChannel.position());
		return entryReferences;
	}
	
	private EntryReference readEntryReferenceFromDisk(FileChannel fileChannel) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(20);
		int bytesRead = fileChannel.read(bb);
		bb.flip();
		final long offset = bb.getLong();
		final long sequenceNumber = bb.getLong();
		final int typeLength = bb.getInt();
		bb = ByteBuffer.allocate(typeLength);
		bytesRead = fileChannel.read(bb);
		bb.flip();
		byte[] bytes = new byte[typeLength];
		bb.get(bytes);
		final String type = new String(bytes);
		return new EntryReference(offset, sequenceNumber, type);
	}
	
	public static EntryReference readEntryReference(ByteBuffer byteBuffer) {
		final long offset = byteBuffer.getLong();
		final long sequenceNumber = byteBuffer.getLong();
		final int typeLength = byteBuffer.getInt();
		ByteBuffer bb = ByteBuffer.allocate(typeLength);
		byte[] bytes = new byte[typeLength];
		// XXX Reading from bb here instead of byteBuffer cannot be correct. Why are we allocate a new ByteBuffer up two lines? Weird.
		bb.get(bytes);
		final String type = new String(bytes);
		return new EntryReference(offset, sequenceNumber, type);		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((entries == null) ? 0 : entries.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecureLog other = (SecureLog) obj;
		if (entries == null) {
			if (other.entries != null)
				return false;
		} else if (!entries.equals(other.entries))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Database [entries=");
		builder.append(entries);
		builder.append("]");
		return builder.toString();
	}

}
