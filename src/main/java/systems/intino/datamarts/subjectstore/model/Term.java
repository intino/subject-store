package systems.intino.datamarts.subjectstore.model;

import systems.intino.datamarts.subjectstore.helpers.EscapeSymbol;

public record Term(String tag, String value) implements Comparable<Term> {

	public Term {
		tag = tag.trim().intern();
		value = normalize(value);
	}

	public static Term of(String str) {
		if (str == null || str.isEmpty()) return null;
		String[] split = str.split("=",2);
		return new Term(split[0], split[1]);
	}

	@Override
	public String toString() {
		return tag + "=" + value;
	}

	public boolean is(String tag) {
		return tag.equals(this.tag);
	}

	public boolean isEmpty() {
		return value == null || value.isEmpty();
	}

	@Override
	public int compareTo(Term term) {
		return toString().compareTo(term.toString());
	}

	private static String normalize(String value) {
		return value.trim()
				.replace(EscapeSymbol.NL, '\n')
				.replace(EscapeSymbol.HT, '\t');
	}


}
