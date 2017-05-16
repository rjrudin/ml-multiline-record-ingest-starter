package org.example;

import com.marklogic.client.helper.LoggingObject;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.util.StringUtils;

public class MultiLinePolicy extends LoggingObject implements RecordSeparatorPolicy {

	private int counter = 0;
	private int rowCount;

	public MultiLinePolicy(int rowCount) {
		this.rowCount = rowCount;
	}

	/**
	 * If the record is empty, it means we're done with the file, so return true.
	 *
	 * @param record
	 * @return
	 */
	@Override
	public boolean isEndOfRecord(String record) {
		return !StringUtils.hasText(record) || counter++ % rowCount == 0;
	}

	/**
	 * Called by Spring Batch on the combined record. No need for any action here.
	 *
	 * @param record
	 * @return
	 */
	@Override
	public String postProcess(String record) {
		return record;
	}

	/**
	 * If we have any text left, then append a comma to it so it's joined with the next row.
	 *
	 * @param record
	 * @return
	 */
	@Override
	public String preProcess(String record) {
		return StringUtils.hasText(record) ? record + "," : record;
	}
}
