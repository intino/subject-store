package systems.intino.datamarts.subjectstore.io;

import java.time.Instant;
import java.util.stream.Stream;

public interface HistoryRegistry extends AutoCloseable {

	int size();

	default boolean isEmpty() { return size() == 0; }

	Stream<Row> tags();

	Stream<Row> instants();

	String ss(int feed);

	double getNumber(int tag, int feed);

	String getText(int tag, int feed);

	Stream<Row> current();

	Stream<Row> getNumbers(int tag, int from, int to);

	Stream<Row> getTexts(int tag, int from, int to);

	void setTag(int id, String label);

	void setTagLastFeed(int id, int feed);

	int nextFeed();

	void put(int tag, Object value);

	void commit();

	void drop();

	Stream<Row> dump();

	interface Row {
		Data at(int index);

		interface Data {
			int asInt();
			long asLong();
			double asDouble();
			Instant asInstant();
			String asString();
		}
	}

}
