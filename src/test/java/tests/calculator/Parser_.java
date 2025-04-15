package tests.calculator;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.calculator.parser.Parser;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class Parser_ {
	@Test
	public void should_parse_constants_variables_and_basic_operations() {
		assertThat(parse("A / (B - 12.5)")).isEqualTo("Div[Var[A], Sub[Var[B], Const[12.5]]]");
		assertThat(parse("12")).isEqualTo("Const[12.0]");
		assertThat(parse("A")).isEqualTo("Var[A]");
		assertThat(parse("A% B")).isEqualTo("Mod[Var[A], Var[B]]");
		assertThat(parse("A+ 12")).isEqualTo("Sum[Var[A], Const[12.0]]");
		assertThat(parse("A * 3 + 12")).isEqualTo("Sum[Mul[Var[A], Const[3.0]], Const[12.0]]");
		assertThat(parse("A * (3 + B)")).isEqualTo("Mul[Var[A], Sum[Const[3.0], Var[B]]]");
		assertThat(parse("A % 4 * (3 + B)")).isEqualTo("Mod[Var[A], Mul[Const[4.0], Sum[Const[3.0], Var[B]]]]");
	}

	@Test
	public void should_ignore_redundant_parentheses() {
		assertThat(parse("(A)")).isEqualTo("Var[A]");
		assertThat(parse("((A))")).isEqualTo("Var[A]");
		assertThat(parse("((A))+(((2)))")).isEqualTo("Sum[Var[A], Const[2.0]]");
	}

	@Test
	public void should_parse_nested_functions() {
		assertThat(parse("SIN(((A+4)*3))")).isEqualTo("F_SIN[Mul[Sum[Var[A], Const[4.0]], Const[3.0]]]");
		assertThat(parse("SIN(A)")).isEqualTo("F_SIN[Var[A]]");
		assertThat(parse("SIN(COS(A))")).isEqualTo("F_SIN[F_COS[Var[A]]]");
	}

	private static String parse(String expression) {
		return new Parser().parse(expression).toString();
	}
}
