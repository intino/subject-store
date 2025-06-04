package systems.intino.datamarts.subjectstore.helpers;

import systems.intino.datamarts.subjectstore.SubjectIndex;
import systems.intino.datamarts.subjectstore.SubjectQuery;
import systems.intino.datamarts.subjectstore.SubjectQuery.OrderType;

import java.util.*;
import java.util.function.Consumer;

public class SubjectQueryParser {
	private final SubjectQuery query;
	private final Map<String, Consumer<String>> mappings;

	public SubjectQueryParser(SubjectIndex index) {
		this.query = index.query();
		this.mappings = createMappings();
	}

	public SubjectQuery parse(String query) {
		String[] parts = query.trim().split("\\s+");
		for (String part : parts)
			process(part);
		return this.query;
	}

	private void process(String value) {
		int i = value.indexOf(':');
		mappings.getOrDefault(i >= 0 ? value.substring(0,i) : value, this::nop)
				.accept(i >= 0 ? value.substring(i+1) : "");
	}

	private void name(String value) {
		process(value);
	}

	private void startsWith(String value) {
		query.nameStartsWith(value);
	}

	private void contains(String value) {
		query.nameContains(value);
	}

	private void endsWith(String value) {
		query.nameEndsWith(value);
	}

	private void type(String value) {
		query.isType(value);
	}

	private void root(String value) {
		query.isRoot();
	}

	private void in(String value) {
		query.isChildOf(value);
	}

	private void under(String value) {
		query.isUnderOf(value);
	}

	private void where(String value) {
		String[] split = value.split("=", 2);
		query.where(split[0]).equals(split[1]);
	}

	private void orderBy(String value) {
		int i = value.indexOf('?');
		OrderType type = orderTypeIn(i >= 0 ? value.substring(i + 1) : "");
		query.orderBy(i >= 0 ? value.substring(0, i) : value, type);
	}

	private static OrderType orderTypeIn(String value) {
		return OrderType.valueOf(normalize(value));
	}

	private static String normalize(String value) {
		List<String> parts = Arrays.asList(value.toLowerCase().split("&"));
		return modeIn(parts) + directionIn(parts);
	}

	private static String directionIn(List<String> parts) {
		return DirectionMap.keySet().stream()
				.filter(parts::contains)
				.map(DirectionMap::get)
				.findFirst()
				.orElse("Ascending");
	}

	private static String modeIn(List<String> parts) {
		return ModeMap.keySet().stream()
				.filter(parts::contains)
				.map(ModeMap::get)
				.findFirst()
				.orElse("Text");
	}

	private Map<String, Consumer<String>> createMappings() {
		Map<String, Consumer<String>> map = new HashMap<>();
		map.put("name", this::name);
		map.put("starts", this::startsWith);
		map.put("contains", this::contains);
		map.put("ends", this::endsWith);
		map.put("type", this::type);
		map.put("root", this::root);
		map.put("in", this::in);
		map.put("under", this::under);
		map.put("where", this::where);
		map.put("order", this::orderBy);
		return map;
	}

	private static final Map<String, String> DirectionMap = Map.of(
			"asc", "Ascending",
			"desc", "Descending"
	);

	private static final Map<String, String> ModeMap = Map.of(
			"text", "Text",
			"num", "Numeric",
			"time", "Instant"
	);

	private void nop(String value) {
	}
}