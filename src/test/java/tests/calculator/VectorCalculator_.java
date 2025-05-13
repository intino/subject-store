package tests.calculator;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.calculator.VectorCalculator;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.DoubleVector;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class VectorCalculator_ {
	@Test
	public void evaluates_constants_and_builtin_functions() {
		VectorCalculator calculator = new VectorCalculator(2, n->null);
		assertThat(calculator.calculate("5").values()).containsExactly(5.0, 5.0);
		assertThat(calculator.calculate("PI").values()).containsExactly(Math.PI, Math.PI);
		assertThat(calculator.calculate("SIN(2)").values()).containsExactly(Math.sin(2), Math.sin(2));
	}

	@Test
	public void supports_variable_definition_and_arithmetic() {
		Map<String, DoubleVector> map = new HashMap<>();
		VectorCalculator calculator = new VectorCalculator(2, map::get);

		map.put("A", calculator.calculate("5"));
		map.put("B", calculator.calculate("2"));
		map.put("C", calculator.calculate("(A * B)"));
		map.put("D", calculator.calculate("A % 2"));
		assertThat(map.get("A").values()).containsExactly(5.0, 5.0);
		assertThat(map.get("B").values()).containsExactly(2.0, 2.0);
		assertThat(calculator.get("C").values()).containsExactly(10.0, 10.0);
		assertThat(calculator.get("D").values()).containsExactly(1.0, 1.0);
	}

	@Test
	public void supports_functions_on_variables() {
		Map<String, DoubleVector> map = new HashMap<>();
		VectorCalculator calculator = new VectorCalculator(2, map::get);

		map.put("A", calculator.calculate("5"));
		map.put("F", calculator.calculate("sin(A)"));

		assertThat(map.get("F").values()).containsExactly(Math.sin(5), Math.sin(5));
	}
}
