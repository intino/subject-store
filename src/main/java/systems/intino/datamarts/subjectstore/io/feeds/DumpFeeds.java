package systems.intino.datamarts.subjectstore.io.feeds;

import systems.intino.datamarts.subjectstore.model.Feed;

import java.io.*;
import java.time.Instant;
import java.util.*;

public class DumpFeeds implements Iterable<Feed>, Closeable {
	private final InputStream is;
	private final Iterator<String> lines;
	private String currentLine;

	public DumpFeeds(InputStream is) {
		this.is = is;
		this.lines = new BufferedReader(new InputStreamReader(is)).lines().iterator();
		this.currentLine = nextLine();
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
		if (currentLine == null) return null;
		assert currentLine.startsWith("[") && currentLine.endsWith("]");
		Feed feed = new Feed(instant(nextLine()), source(nextLine()));
		while (true) {
			currentLine = nextLine();
			if (currentLine == null || isHeader(currentLine)) break;
			put(feed, currentLine.split("="));
		}
		return feed;
	}

	private void put(Feed feed, String[] tuple) {
		put(feed, tuple[0], tuple[1]);
	}

	private static void put(Feed feed, String tag, String value) {
		try {
			feed.put(tag, Double.parseDouble(value));
		}
		catch (NumberFormatException e) {
			feed.put(tag, value);
		}
	}

	private Instant instant(String s) {
		if (s == null) return null;
		String[] tuple = s.split("=");
		assert tuple[0].equals("ts");
		return Instant.parse(tuple[1]);
	}

	private String source(String s) {
		if (s == null) return null;
		String[] tuple = s.split("=");
		assert tuple[0].equals("ss");
		return tuple[1];
	}

	private String nextLine() {
		return lines.hasNext() ? lines.next() : null;
	}

	private boolean isHeader(String line) {
		return line.startsWith("[") && line.endsWith("]");
	}


	@Override
	public void close() throws IOException {
		is.close();
	}
}
