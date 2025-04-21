package systems.intino.datamarts.subjectstore.io.feeds;

import systems.intino.datamarts.subjectstore.io.Feeds;
import systems.intino.datamarts.subjectstore.model.Feed;

import java.time.Instant;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;

import static systems.intino.datamarts.subjectstore.io.HistoryRegistry.Row;

public class RegistryFeeds implements Feeds {
	private final Iterator<Row> iterator;
	private final Function<Integer, String> tagNames;
	private Row row;

	public RegistryFeeds(Stream<Row> stream, Function<Integer, String> tagNames) {
		this.iterator = stream.iterator();
		this.tagNames = tagNames;
		this.row = nextRow();
	}

	@Override
	public Iterator<Feed> iterator() {
		return new Iterator<>() {
			Feed feed = nextFeed();
			@Override
			public boolean hasNext() {
				return feed != null;
			}

			@Override
			public Feed next() {
				try {
					return feed;
				}
				finally {
					feed = nextFeed();
				}
			}
		};
	}

	private Feed nextFeed() {
		if (row == null) return null;
		int id = row.at(1).asInt();
		Instant ts = instantAt(row);
		String ss = sourceAt(iterator.next());
		Feed feed =new Feed(ts, ss);
		while (true) {
			row = nextRow();
			if (row == null) break;
			if (row.at(1).asInt() != id) break;
			feed.put(tagAt(row), valueAt(row));
		}
		return feed;
	}

	private Row nextRow() {
		return iterator.hasNext() ? iterator.next() : null;
	}

	private Instant instantAt(Row row) {
		assert row.at(2).asInt() == 0;
		return row.at(3).asInstant();
	}

	private String sourceAt(Row row) {
		assert row.at(2).asInt() == 1;
		return row.at(4).asString();
	}

	private Object valueAt(Row row) {
		String value = row.at(4).asString();
		return value != null ? value : row.at(3).asDouble();
	}

	private String tagAt(Row row) {
		return tagNames.apply(row.at(2).asInt());
	}

}
