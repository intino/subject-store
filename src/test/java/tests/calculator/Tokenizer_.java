package tests.calculator;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.calculator.parser.Token;
import systems.intino.datamarts.subjectstore.calculator.parser.Tokenizer;

import static org.assertj.core.api.Assertions.assertThat;
import static systems.intino.datamarts.subjectstore.calculator.parser.Token.Type.*;

@SuppressWarnings("ALL")
public class Tokenizer_ {

	@Test
	public void should_tokenize() {
		Tokenizer tokenizer = new Tokenizer("12 + 3.3 * SIN(AX)");
		assertThat(tokenizer.tokens()).containsExactly(
				new Token(NUMBER, 0, "12"),
				new Token(OPERATOR, 3, "+"),
				new Token(NUMBER, 5, "3.3"),
				new Token(OPERATOR, 9, "*"),
				new Token(IDENTIFIER, 11, "SIN"),
				new Token(BRACE_OPEN, 14, "("),
				new Token(IDENTIFIER, 15, "AX"),
				new Token(BRACE_CLOSE, 17, ")")
		);
	}
}
