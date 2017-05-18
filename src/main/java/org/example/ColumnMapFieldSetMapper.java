package org.example;

import com.marklogic.client.helper.LoggingObject;
import org.apache.commons.lang3.StringEscapeUtils;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.util.Assert;
import org.springframework.validation.BindException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * FieldSetMapper that operates on a ColumnMap.
 */
public class ColumnMapFieldSetMapper extends LoggingObject implements FieldSetMapper<Map<String, Object>> {

	private String[] fieldNames;
	private String recordName = "changeme";

	@Override
	public Map<String, Object> mapFieldSet(FieldSet fieldSet) throws BindException {
		Assert.notEmpty(fieldNames, "fieldNames must be set so this class knows how to populate a column map and how many fields there are.");
		List<Map<String, Object>> records = new ArrayList<>();

		int fieldCount = fieldSet.getFieldCount();
		int rowCount = fieldCount / fieldNames.length;
		for (int i = 0; i < rowCount; i++) {
			Map<String, Object> record = new HashMap<>();
			int index = fieldNames.length * i;
			for (int j = 0; j < fieldNames.length; j++) {
				record.put(fieldNames[j], readString(fieldSet, index + j));
			}
			records.add(record);
		}
		Map<String, Object> map = new HashMap<>();
		map.put(recordName, records);
		return map;
	}

	protected String readString(FieldSet fieldSet, int index) {
		String value = fieldSet.readString(index);
		return StringEscapeUtils.escapeXml11(value);
	}

	public void setFieldNames(String[] fieldNames) {
		this.fieldNames = fieldNames;
	}

	public void setRecordName(String recordName) {
		this.recordName = recordName;
	}
}
