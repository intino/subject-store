package systems.intino.datamarts.subjectstore.calculator.parser;

import systems.intino.datamarts.subjectstore.calculator.Expression;
import systems.intino.datamarts.subjectstore.calculator.expressions.Constant;
import systems.intino.datamarts.subjectstore.calculator.expressions.NamedFunction;
import systems.intino.datamarts.subjectstore.calculator.expressions.Variable;
import systems.intino.datamarts.subjectstore.calculator.expressions.operators.*;
import systems.intino.datamarts.subjectstore.calculator.expressions.operators.Module;

import java.util.List;
import java.util.Stack;

import static java.lang.Double.parseDouble;
import static systems.intino.datamarts.subjectstore.calculator.parser.Parser.Operator.Type.Binary;
import static systems.intino.datamarts.subjectstore.calculator.parser.Parser.Operator.Type.Unary;
import static systems.intino.datamarts.subjectstore.calculator.parser.Token.Type.*;

public class Parser {
	private final Stack<Expression> output = new Stack<>();
	private final Stack<Token> pendingTokens = new Stack<>();
	private final Stack<Token> functions = new Stack<>();
	private Token lastToken = null;

	public Expression parse(String expression) {
		return parse(new Tokenizer(expression).tokens());
	}

	private Expression parse(List<Token> tokens) {
		for (Token token : tokens) {
			switch (token.type()) {
				case NUMBER -> processNumber(token);
				case IDENTIFIER -> processIdentifier(token);
				case OPERATOR -> processOperator(token);
				case BRACE_OPEN -> openBracket(token);
				case BRACE_CLOSE -> closeBracket();
			}
			lastToken = token;
		}
		while (!pendingTokens.isEmpty()) buildExpression();
		return output.pop();
	}

	private void processNumber(Token token) {
		output.push(new Constant(parseDouble(token.value())));
	}

	private void processIdentifier(Token token) {
		output.push(new Variable(token.value()));
	}

	private void processOperator(Token token) {
		if (precedenceOf(token) < precedenceOf(pendingToken()))
			buildExpression();
		pendingTokens.push(token);
	}

	private void openBracket(Token token) {
		createFunction();
		pendingTokens.push(token);
	}

	private void closeBracket() {
		buildExpression();
		Token token = pendingTokens.pop();
		assert token.type() == BRACE_OPEN;
	}

	private void createFunction() {
		if (lastToken == null) return;
		if (lastToken.type() != IDENTIFIER) return;
		output.pop();
		functions.push(lastToken);
	}

	private Token pendingToken() {
		return pendingTokens.isEmpty() ? null :pendingTokens.peek();
	}

	private int precedenceOf(Token token) {
		return token != null ? operatorOf(token).precedence : Integer.MIN_VALUE;
	}

	private boolean isPendingTokenBraceOpen() {
		return pendingTokens.peek().type() == BRACE_OPEN;
	}

	private void buildExpression() {
		if (isPendingTokenBraceOpen())
			buildFunction();
		else
			buildOperation();
	}

	private void buildFunction() {
		if (functions.isEmpty()) return;
		buildFunctionWith(functions.pop().value());
	}

	private void buildFunctionWith(String name) {
		NamedFunction function = new NamedFunction(name, output.pop());
		output.push(function);
	}

	private void buildOperation() {
		buildOperationWith(pendingTokens.pop());
	}

	private void buildOperationWith(Token token) {
		buildOperationWith(operatorOf(token));
	}

	private void buildOperationWith(Operator operator) {
		switch (operator.type) {
			case Binary -> output.push(binaryExpressionWith(operator));
			case Unary -> output.push(unaryExpressionWith(operator));
		}
	}

	private Expression unaryExpressionWith(Operator operator) {
		return operator == Operator.Factorial ? Factorial.with(output.pop()) : null;
	}

	private Expression binaryExpressionWith(Operator operator) {
		return switch (operator) {
			case Sum -> Sum.with(output.pop(), output.pop());
			case Minus -> Subtraction.with(output.pop(), output.pop());
			case Multiply -> Product.with(output.pop(), output.pop());
			case Divide -> Division.with(output.pop(), output.pop());
			case Module -> Module.with(output.pop(), output.pop());
			default -> null;
		};
	}

	private Operator operatorOf(Token token) {
		for (Operator operator : Operator.values())
			if (operator.matches(token)) return operator;
		return Operator.Unknown;
	}

	public enum Operator {
		Sum("+", 1, Binary), Minus("-", 1, Binary),
		Multiply("*", 2, Binary), Divide("/", 2, Binary), Module("%", 2, Binary),
		Factorial("!", 3, Unary), Unknown("...", 0, Unary);

		public final String symbol;
		public final int precedence;
		public final Type type;

		Operator(String symbol, int precedence, Type type) {
			this.symbol = symbol;
			this.precedence = precedence;
			this.type = type;
		}

		boolean matches(Token token) {
			return token.value().equals(symbol);
		}

		public enum Type {
			Binary, Unary
		}

	}

}
