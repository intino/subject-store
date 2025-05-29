package systems.intino.datamarts.subjectstore.calculator.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.lang.Character.isDigit;
import static java.lang.Character.isWhitespace;
import static systems.intino.datamarts.subjectstore.calculator.parser.Token.Type.*;

public class Tokenizer {
	private final String input;
	private int index = 0;
	private char currentChar;

	public Tokenizer(String input) {
		this.input = input;
		this.nextChar();
	}

	public List<Token> tokens() {
		List<Token> tokens = new ArrayList<>();
		while (true) {
			Token token = nextToken();
			if (token == null) break;
			tokens.add(token);
		}
		return tokens;
	}


	private Token nextToken() {
		skipBlanks();
		if (isEndOfInput()) return null;
		if (isNumber()) return tokenNumber();
		if (isStartingIdentifier()) return tokenIdentifier();

		try {
			if (isBraceOpen()) return token(BRACE_OPEN);
			if (isBraceClose()) return token(BRACE_CLOSE);
			if (isOperator()) return token(OPERATOR);
		}
		finally {
			nextChar();
		}
		return token(UNKNOWN);
	}

	private static final Set<Character> Characters = Set.of('+','-','*','/','^','%');

	private boolean isOperator() {
		return Characters.contains(currentChar);
	}
	private boolean isBraceClose() {
		return currentChar == ')';
	}

	private boolean isBraceOpen() {
		return currentChar == '(';
	}

	private Token token(Token.Type type) {
		return new Token(type, index - 1, currentChar + "");
	}

	private Token tokenIdentifier()  {
		int startIndex = index - 1;
		StringBuilder name = new StringBuilder();
		while (!isEndOfInput() && isIdentifier()) {
			name.append(currentChar);
			nextChar();
		}
		return new Token(IDENTIFIER, startIndex, name.toString());

	}

	private Token tokenNumber() {
		int startIndex = index - 1;
		StringBuilder value = new StringBuilder();
		while (!isEndOfInput() && isNumber()) {
			value.append(currentChar);
			nextChar();
		}
		return new Token(NUMBER, startIndex, value.toString());
	}

	private void skipBlanks() {
		while (!isEndOfInput() && isWhitespace(currentChar))
			nextChar();
	}

	private void nextChar() {
		currentChar = popChar();
	}

	private boolean isEndOfInput() {
		return currentChar == Character.MAX_VALUE;
	}

	private boolean isStartingIdentifier() {
		return Character.isLetter(currentChar) || currentChar == '_';
	}

	private boolean isIdentifier() {
		return Character.isAlphabetic(currentChar) || "_-#.:".indexOf(currentChar) >= 0;
	}

	private boolean isNumber() {
		return isDigit(currentChar) || currentChar == '.' && isDigit(peekChar());
	}

	private char popChar() {
		return index < input.length() ? input.charAt(index++) : Character.MAX_VALUE;
	}

	private char peekChar() {
		return index < input.length() ? input.charAt(index) : Character.MAX_VALUE;

	}


}
