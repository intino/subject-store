package systems.intino.datamarts.subjectstore.view.history;

import systems.intino.datamarts.subjectstore.view.Stats;

import java.util.HashMap;
import java.util.Map;

public interface Column {
	String name();

	record DoubleColumn(String name, double[] values) implements Column {

	}

	record StringColumn(String name, String[] values, Stats stats) implements Column {
		public StringColumn(String name, String[] values) {
			this(name, values, Stats.of(values));
		}

		@Override
		public String toString() {
			return name;
		}

	}

}
