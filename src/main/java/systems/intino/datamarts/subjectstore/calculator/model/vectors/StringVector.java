package systems.intino.datamarts.subjectstore.calculator.model.vectors;

import systems.intino.datamarts.subjectstore.calculator.model.Vector;

import java.util.Arrays;

public record StringVector(String[] values) implements Vector<String> {
	@Override
	public String get(int index) {
		return values[index];
	}

	@Override
	public int size() {
		return values.length;
	}

	@Override
	public String toString() {
		return Arrays.toString(values);
	}

}
