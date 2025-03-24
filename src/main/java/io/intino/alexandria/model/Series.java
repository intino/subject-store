package io.intino.alexandria.model;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public interface Series<T> extends Iterable<Point<T>> {
	Instant from();
	Instant to();
	default Duration duration() { return Duration.between(from(), to()); }

	int count();
	boolean isEmpty();
	Stream<Point<T>> stream();

	abstract class Raw<X> implements Series<X> {
		private final Instant from;
		private final Instant to;
		private final List<Point<X>> points;

		public Raw(Instant from, Instant to, List<Point<X>> points) {
			this.from = from;
			this.to = to;
			this.points = points;
		}

		@Override
		public Instant from() {
			return from;
		}

		@Override
		public Instant to() {
			return to;
		}

		@Override
		public int count() {
			return points().size();
		}

		@Override
		public boolean isEmpty() {
			return points.isEmpty();
		}

		@Override
		public Stream<Point<X>> stream() {
			return points.stream();
		}

		@Override
		public Iterator<Point<X>> iterator() {
			return points.iterator();
		}

		public List<Point<X>> points() {
			return points;
		}

		@Override
		public int hashCode() {
			return Objects.hash(from, to, points);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof Raw<?> raw)) return false;
			return Objects.equals(from, raw.from) && Objects.equals(to, raw.to) && Objects.equals(points, raw.points);
		}
	}

	abstract class Segment<X> implements Series<X> {
		private final Instant from;
		private final Instant to;
		private final Series<X> parent;

		public Segment(Instant from, Instant to, Series<X> parent) {
			this.from = from;
			this.to = to;
			this.parent = parent;
		}

		public Instant from() {
			return from;
		}

		public Instant to() {
			return to;
		}

		public int count() {
			return (int) stream().count();
		}

		public boolean isEmpty() {
			return stream().findAny().isEmpty();
		}

		public Iterator<Point<X>> iterator() {
			return stream().iterator();
		}

		public Stream<Point<X>> stream() {
			return parent.stream().filter(this::contains);
		}

		private boolean contains(Point<X> point) {
			return contains(point.instant());
		}

		private boolean contains(Instant instant) {
			return !instant.isBefore(from) && instant.isBefore(to);
		}

		@Override
		public int hashCode() {
			return Objects.hash(from, to, parent);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof Segment<?> segment)) return false;
			return Objects.equals(from, segment.from) && Objects.equals(to, segment.to) && Objects.equals(parent, segment.parent);
		}
	}

}
