package org.example;

import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;

/**
 * Captures the column names from the first row and hands them off for ColumnMapFieldSetMapper to utilize.
 */
public class ColumnMapLineCallbackHandler implements LineCallbackHandler {

	private ColumnMapFieldSetMapper mapper;

	public ColumnMapLineCallbackHandler(ColumnMapFieldSetMapper mapper) {
		this.mapper = mapper;
	}

	@Override
	public void handleLine(String line) {
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_COMMA);
		FieldSet fs = tokenizer.tokenize(line);
		mapper.setFieldNames(fs.getValues());
	}
}
