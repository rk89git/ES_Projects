package com.db.result.service;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.stereotype.Service;

import com.db.common.constants.Constants.Result;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.utils.DBConfig;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

@Service
public class ResultExecutorService {

	private static final Logger LOG = LogManager.getLogger(ResultExecutorService.class);

	private ObjectMapper mapper = new ObjectMapper();

	private Map<String, Database> cacheDBInstances = new ConcurrentHashMap<>();

	private Map<String, Index> cacheIndexInstances = new ConcurrentHashMap<>();

	private DBConfig config = DBConfig.getInstance();

	public String getResultv2(String jsonData) {
		try {
			Map<String, Object> jsonMap = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});

			String year = (String) jsonMap.get(Result.YEAR);
			String standard = (String) jsonMap.get(Result.STANDARD);
			String board = (String) jsonMap.get(Result.BOARD);
			String rollNo = (String) jsonMap.get(Result.ROLL_NO);
			String field = (String) jsonMap.get(Result.FIELD);

			if (StringUtils.isBlank(year) || StringUtils.isBlank(standard) || StringUtils.isBlank(board)
					|| StringUtils.isBlank(rollNo)) {
				return "Pass all parameters- year, standard, board & ROLL_NO";
			}

			String fileName = board + "_" + standard + "_" + year;

			Row row = getResultData(fileName, Collections.singletonMap(field, rollNo), field);

			if (row == null || row.isEmpty()) {
				return "No record Found";
			} else {
				return mapper.writeValueAsString(row);
			}

		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
	}

	public String getResult(String jsonData) {
		try {
			Map<String, Object> jsonMap = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});

			String year = (String) jsonMap.get(Result.YEAR);
			String standard = (String) jsonMap.get(Result.STANDARD);
			String board = (String) jsonMap.get(Result.BOARD);
			List<?> fields = (List<?>) jsonMap.get("fields");

			if (StringUtils.isBlank(year) || StringUtils.isBlank(standard) || StringUtils.isBlank(board)
					|| (fields == null || fields.isEmpty())) {
				return "Pass all parameters- year, standard, board & fields";
			}

			String fileName = board + "_" + standard + "_" + year;

			Row row = getResultData(fileName, fields);

			if (row == null || row.isEmpty()) {
				return "No record Found";
			} else {
				return mapper.writeValueAsString(row);
			}

		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
	}

	private synchronized Row getResultData(String fileName, List<?> fields) {

		Map<String, Object> queryMap = new HashMap<>();
		for (Object field : fields) {
			Map<?, ?> ref = (Map<?, ?>) field;
			queryMap.put(ref.get("field").toString(), ref.get("value"));
		}

		Row row = null;
		try {
			row = CursorBuilder.findRow(getIndex(fileName, queryMap.keySet().toArray(new String[0])), queryMap);
		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
		return row;
	}

	private synchronized Row getResultData(String fileName, Map<String, Object> queryMap, String field)
			throws IOException {
		Row row = null;

		try {
			row = CursorBuilder.findRow(getIndex(fileName, field), queryMap);
		} catch (ClosedChannelException e) {
			row = CursorBuilder.findRow(getIndex(fileName, field), queryMap);
		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
		return row;
	}

	private Database createMSAccessConnection(String file) {
		LOG.info("Creating connection for file: " + file);
		try {
			File resultFile = new File(file);

			if (resultFile.exists()) {
				return DatabaseBuilder.open(resultFile);
			} else {
				throw new DBAnalyticsException("MsAccess Connection Failed: File Not found " + file);
			}
		} catch (Exception e) {
			LOG.error("ERROR: Failed to open db file: " + file, e);
			throw new DBAnalyticsException(e);
		}
	}

	public Table getTable(String fileName) {
		long startTime = System.currentTimeMillis();
		String filePath = config.getString("result.file.path") + fileName;
		Set<String> tables = null;
		Database db = null;

		try {
			if (cacheDBInstances.containsKey(fileName)) {
				db = cacheDBInstances.get(fileName);
			} else {
				db = createMSAccessConnection(filePath);
				cacheDBInstances.put(fileName, db);
				LOG.info("Time taken to create db connection: " + (System.currentTimeMillis() - startTime) + " ms.");
			}
			tables = db.getTableNames();

			if (tables == null || tables.isEmpty()) {
				LOG.error("Database is empty for file: " + filePath);
				throw new DBAnalyticsException("Database is empty for file " + filePath);
			}

			return db.getTable(tables.iterator().next());
		} catch (Exception e) {
			LOG.error("Failed to open database: ", e);
			throw new DBAnalyticsException(e);
		}
	}

	public Index getIndex(String fileName, String... fields) {
		long startTime = System.currentTimeMillis();

		String indexName = String.join("_", fields);

		try {
			if (cacheIndexInstances.containsKey(fileName)) {
				return cacheIndexInstances.get(fileName);
			} else {
				Index idx = getOrCreateIndex(fileName, indexName, fields);
				cacheIndexInstances.put(fileName, idx);
				LOG.info("Time to get index: " + (System.currentTimeMillis() - startTime));
				return idx;
			}
		} catch (Exception e) {
			LOG.error("Failed to retreive index from file: " + fileName, e);
			throw new DBAnalyticsException(e);
		}
	}

	public Index getOrCreateIndex(String fileName, String indexName, String... fields) {

		Table table = null;
		Index idx = null;
		try {
			table = getTable(fileName);
			idx = table.getIndex(indexName);
			return idx;
		} catch (IllegalArgumentException iae) {
			LOG.info("No index found for name: " + indexName + ". Creating index: " + indexName + " with fields: "
					+ Arrays.toString(fields));
		} catch (Exception e) {
			LOG.error("Failure: ", e);
			throw new DBAnalyticsException("Failure: ", e);
		}

		IndexBuilder builder = new IndexBuilder(indexName);
		builder.addColumns(fields);

		try {
			idx = builder.addToTable(table);
		} catch (IOException ioe) {
			LOG.error("ERROR: ", ioe);
			throw new DBAnalyticsException("Failed to create index: " + indexName, ioe);
		}

		return idx;
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		ResultExecutorService e = new ResultExecutorService();

		String json = "{\"board\":\"mp\",\"standard\":\"12\",\"year\":\"2018\",\"fields\":[{\"field\":\"ROLLNO\",\"value\":\"287899501\"},{\"field\":\"APPNO\",\"value\":\"27820033\"}]}";
		String json2 = "{\"board\":\"mp\",\"standard\":\"12\",\"year\":\"2018\",\"fields\":[{\"field\":\"ROLLNO\",\"value\":\"287899501\"}]}";

		// Index idx = new
		// IndexBuilder("ROLLNO").addColumns("ROLLNO").addToTable(e.getTable("mp_12_2018"));
		// System.out.println("Time to index: " + (System.currentTimeMillis() -
		// startTime) + " ms.");
		// startTime = System.currentTimeMillis();

		System.out.println(e.getResult(json2));
		LOG.info("Execution: " + (System.currentTimeMillis() - startTime) + " ms.");
		startTime = System.currentTimeMillis();
		System.out.println(e.getResult(json));
		LOG.info("Execution: " + (System.currentTimeMillis() - startTime) + " ms.");

		// Table t = e.getTable("mp_12_2018_test");

		// Map<String, Integer> query = new HashMap<>();
		// query.put("ROLLNO", 287899501);
		// query.put("APPNO", 27820033);

		// Row r = CursorBuilder.findRow(t.getIndex("rollno"),
		// Collections.singletonMap("ROLLNO", 287899501));
		// Row r = CursorBuilder.findRow(t.getIndex("ROLLNO"), query);
		// System.out.println(e.mapper.writeValueAsString(r));
		// startTime = System.currentTimeMillis();
		// System.out.println(e.getResult(json));
		//
	}
}
