package v1;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class EntryReference {
	Entry entry = null;
	private final long offset;
	private final long sequenceNumber;
	private final String type;
	
	public EntryReference(final long offset, final long sequenceNumber, final String type) {
		this.offset = offset;
		this.sequenceNumber = sequenceNumber;
		this.type = type;
	}
	
	public void writeTo(FileChannel fileChannel) throws IOException {
		ByteBuffer bb = this.toByteBuffer();
		bb.flip();
		while (bb.hasRemaining()) {
			fileChannel.write(bb);
		}
	}
	
	public long getOffset() {
		return this.offset;
	}
	
	public long getSequenceNumber() {
		return this.sequenceNumber;
	}
	
	public String getType() {
		return this.type;
	}

	public ByteBuffer toByteBuffer() {
		// offset:8 + sequenceNumber:8 + type.length:4 + type.length()
		byte[] typeBytes = type.getBytes();
		ByteBuffer bb = ByteBuffer.allocate(20+typeBytes.length);
		bb.putLong(offset);
		bb.putLong(sequenceNumber);
		bb.putInt(typeBytes.length);
		bb.put(typeBytes);
		//bb.put((byte) 0x00);
		return bb;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("EntryReference [offset=");
		builder.append(offset);
		builder.append(", sequenceNumber=");
		builder.append(sequenceNumber);
		builder.append(", type=");
		builder.append(type);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (offset ^ (offset >>> 32));
		result = prime * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		EntryReference other = (EntryReference) obj;
		if (offset != other.offset)
			return false;
		if (sequenceNumber != other.sequenceNumber)
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}
	
}
