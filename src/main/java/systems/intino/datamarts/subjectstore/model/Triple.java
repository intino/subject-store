package systems.intino.datamarts.subjectstore.model;

import systems.intino.datamarts.subjectstore.helpers.EscapeSymbol;

public record Triple(String subject, String tag, String value) {

	public static Triple of(String str) {
		int i = str.indexOf('\t');
		int j = str.indexOf('\t', i + 1);
		return new Triple(str.substring(0, i), str.substring(i+1, j), str.substring(j+1));
	}

	public String term() {
		return tag + '=' + value;
	}

	@Override
	public String toString() {
		return subject + "\t" + tag + "\t" + escape(value);
	}

	private static String escape(String str) {
		return str
				.replace('\n', EscapeSymbol.NL)
				.replace('\t',EscapeSymbol.HT);
	}
}
