package systems.intino.datamarts.subjectstore.calculator.parser;

public record Token(Type type, int startPosition, String value) {
	public enum Type {
		BRACE_OPEN, BRACE_CLOSE,
		NUMBER, IDENTIFIER, OPERATOR,
		UNKNOWN
	}
}
