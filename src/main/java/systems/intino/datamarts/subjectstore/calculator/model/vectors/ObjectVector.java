package systems.intino.datamarts.subjectstore.calculator.model.vectors;

import systems.intino.datamarts.subjectstore.calculator.model.Vector;

import java.util.Arrays;

public record ObjectVector(Object[] values) implements Vector<Object> {
	@Override
	public Object get(int index) {
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
