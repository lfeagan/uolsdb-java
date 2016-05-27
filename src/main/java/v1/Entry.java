package v1;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class Entry {

	private final SecureLog database;
	private long offset;
	private final long sequenceNumber;
	private final List<EntryReference> entryReferences = new LinkedList<EntryReference>();
	private final byte[] payload;
	private final byte[] signature;

	public Entry(final SecureLog database, final Long sequenceNumber) {
		this(database, sequenceNumber, null);
	}

	public Entry(final SecureLog database, final Long sequenceNumber, final List<EntryReference> entryReferences) {
		this.database = database;
		this.sequenceNumber = sequenceNumber;
		if (entryReferences != null) {
			this.entryReferences.addAll(entryReferences);
			for (EntryReference er : this.entryReferences) {
				er.entry = this;
			}
		}
		this.payload = new byte[100];
		for (int i=0; i < payload.length; ++i) {
			payload[i] = (byte) i;
		}
		this.signature = new byte[0];
	}

	public void addReferenceTo(Entry entry, String type) {
		// verify entry being referenced has a sequence number before this
		this.entryReferences.add(new EntryReference(entry.getOffset(), entry.getSequenceNumber(), type));
	}

	public SecureLog getDatabase() {
		return database;
	}

	public long getOffset() {
		return offset;
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public List<EntryReference> getEntryReferences() {
		return entryReferences;
	}

	public byte[] getPayload() {
		return payload;
	}

	public byte[] getSignature() {
		return signature;
	}
	
	public void write() throws IOException {
		try (FileChannel fileChannel = database.getWritableFileChannel();) {
			this.offset = fileChannel.size();
			fileChannel.position(this.offset);
			ByteBuffer bb = this.toByteBuffer();
			bb.flip();
			while (bb.hasRemaining()) {
				fileChannel.write(bb);
			}
			fileChannel.force(true);
		}
		database.entryIndex.addEntry(this);
	}

	public void writeTo(FileChannel fileChannel) throws IOException {
		this.offset = fileChannel.position();
		ByteBuffer bb = this.toByteBuffer();
		bb.flip();
		while (bb.hasRemaining()) {
			fileChannel.write(bb);
		}
	}

	public ByteBuffer toByteBuffer() {
		int erbbsSize = 0;
		ByteBuffer[] erbbs = new ByteBuffer[entryReferences.size()];
		for (int i=0; i < entryReferences.size(); ++i) {
			final ByteBuffer erbb = entryReferences.get(i).toByteBuffer();
			erbb.flip();
			erbbs[i] = erbb;
			erbbsSize += erbb.capacity();
		}
		// length:4 + sequenceNumber:8 + entryReferences.length:4 + sum(entryReference[])
		final int length = 16 + erbbsSize + 4 + payload.length + signature.length;
		ByteBuffer bb = ByteBuffer.allocate(length);
		bb.putInt(length);
		bb.putLong(sequenceNumber);
		bb.putInt(entryReferences.size());
		for (int i=0; i < erbbs.length; ++i) {
			bb.put(erbbs[i]);
		}
		bb.putInt(payload.length);
		bb.put(payload);
		//		bb.putInt(signatureLength);
		//		bb.put(signature);
		return bb;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((entryReferences == null) ? 0 : entryReferences.hashCode());
		result = prime * result + (int) (offset ^ (offset >>> 32));
		result = prime * result + Arrays.hashCode(payload);
		result = prime * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
		result = prime * result + Arrays.hashCode(signature);
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
		Entry other = (Entry) obj;
		if (entryReferences == null) {
			if (other.entryReferences != null)
				return false;
		} else if (!entryReferences.equals(other.entryReferences))
			return false;
		if (offset != other.offset)
			return false;
		if (!Arrays.equals(payload, other.payload))
			return false;
		if (sequenceNumber != other.sequenceNumber)
			return false;
		if (!Arrays.equals(signature, other.signature))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Entry [offset=");
		builder.append(offset);
		builder.append(", sequenceNumber=");
		builder.append(sequenceNumber);
		builder.append(", entryReferences=");
		builder.append(entryReferences);
		builder.append(", payload=");
		builder.append(Arrays.toString(payload));
		builder.append(", signature=");
		builder.append(Arrays.toString(signature));
		builder.append("]");
		return builder.toString();
	}

}
