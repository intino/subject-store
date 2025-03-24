package io.intino.alexandria;

import io.intino.alexandria.io.Registry;
import io.intino.alexandria.io.Transaction;
import io.intino.alexandria.model.Instants;
import io.intino.alexandria.model.Point;
import io.intino.alexandria.io.registries.SqliteRegistry;
import io.intino.alexandria.model.series.Sequence;
import io.intino.alexandria.model.series.Signal;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static io.intino.alexandria.model.Instants.BigBang;
import static io.intino.alexandria.model.Instants.Legacy;

public class SubjectStore implements Closeable {
	private final Registry registry;

	public SubjectStore(File file) {
		this.registry = new SqliteRegistry(file);
	}

	public String name() {
		return registry.name();
	}

	public int feeds() {
		return registry.feeds();
	}

	public Instant from() {
		return registry.instants().get(0);
	}

	public Instant to() {
		return registry.instants().get(feeds()-1);
	}

	public boolean legacyExists() {
		return registry.instants().contains(Legacy);
	}

	public boolean bigbangExists() {
		return registry.instants().contains(BigBang);
	}

	public boolean legacyPending() {
		return legacyExists() && !bigbangExists();
	}

	public List<Instant> instants() {
		return registry.instants();
	}

	public List<String> tags() {
		return registry.tags();
	}

	public String ss(int feed) {
		return registry.ss(feed);
	}

	public NumericalQuery numericalQuery(String tag) {
		return new NumericalQuery(tag);
	}

	public CategoricalQuery categoricalQuery(String tag) {
		return new CategoricalQuery(tag);
	}

	public boolean exists(String tag) {
		return tags().contains(tag);
	}


	public class CategoricalQuery {
		private final String tag;

		public CategoricalQuery(String tag) {
			this.tag = tag;
		}

		public Point<String> current() {
			return registry.readText(tag);
		}

		public Sequence sequence(Instant from, Instant to) {
			return new Sequence.Raw(from, to, registry.readTexts(tag, from, to));
		}

		public Sequence sequence(Instants.TimeSpan span) {
			return sequence(span.from(), span.to());
		}

		@Override
		public String toString() {
			return "TextQuery(" + tag + ')';
		}
	}

	public class NumericalQuery {
		private final String tag;

		public NumericalQuery(String tag) {
			this.tag = tag;
		}

		public Point<Long> current() {
			return registry.readLong(tag);
		}

		public Signal signal(Instant from, Instant to) {
			return new Signal.Raw(from, to, registry.readLongs(tag, from, to));
		}

		public Signal signal(Instants.TimeSpan span) {
			return signal(span.from(), span.to());
		}

		@Override
		public String toString() {
			return "LongQuery(" + tag + ')';
		}
	}

	public Feed feed(Instant instant, String source) {
		return feed(new Transaction(instant, source));
	}

	private Feed feed(Transaction transaction) {
		return new Feed() {
			@Override
			public Feed add(String tag, String value) {
				transaction.put(tag, value);
				return this;
			}

			@Override
			public Feed add(String tag, long value) {
				transaction.put(tag, value);
				return this;
			}

			@Override
			public void execute() {
				registry.register(transaction);
			}
		};
	}

	public interface Feed {
		Feed add(String name, long value);
		Feed add(String name, String value);
		void execute();
	}

	@Override
	public String toString() {
		return registry.name();
	}

	@Override
	public void close() throws IOException {
		registry.close();
	}

	public static class RegistryException extends RuntimeException {
		public RegistryException(Exception exception) {
			super(exception);
		}
	}
}
