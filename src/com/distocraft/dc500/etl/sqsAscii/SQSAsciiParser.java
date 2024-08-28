package com.distocraft.dc500.etl.sqsAscii;

import com.distocraft.dc5000.etl.parser.Main;
import com.distocraft.dc5000.etl.parser.MeasurementFile;
import com.distocraft.dc5000.etl.parser.Parser;
import com.distocraft.dc5000.etl.parser.SourceFile;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SQSAsciiParser implements Parser {
	InputStream in;

	BufferedReader reader;

	String result;

	String object;

	String event;

	HashMap<String, String> commonKeys = null;

	HashMap<String, String> generatedData = new HashMap<String, String>();

	HashMap<String, String> objectMeasurementData = new HashMap<String, String>();

	LinkedHashMap<String, HashMap<String, String>> objectVectorData = new LinkedHashMap<String, HashMap<String, String>>();

	HashMap<String, String> tmpKeys = null;

	HashSet<String> vectorKeys = new HashSet<String>();

	Vector<String> data = null;

	int blockCount = 0;

	boolean isKeyRow = true;

	boolean isVectorRow = false;

	boolean isObjectHeaderRow = false;

	boolean isRopHeaderRow = false;

	private final String JVM_TIMEZONE = (new SimpleDateFormat("Z")).format(new Date());

	private Main main;

	private String techPack;

	private String setType;

	private String setName;

	private String workerName;

	private int status = 0;

	private Logger logger;

	private HashMap<String, MeasurementFile> openFiles;

	private SourceFile sourceFile;

	String tagIdField;

	String vectorIndex;

	public void init(Main paramMain, String paramString1, String paramString2, String paramString3,
			String paramString4) {
		this.main = paramMain;
		this.techPack = paramString1;
		this.setType = paramString2;
		this.setName = paramString3;
		this.workerName = paramString4;
		this.logger = Logger.getLogger("etl." + paramString1 + "." + paramString2 + "." + paramString3
				+ ".parser.SQSAsciiParser." + paramString4);
		this.status = 1;
	}

	public void parse(SourceFile paramSourceFile, String paramString1, String paramString2, String paramString3)
			throws Exception {
		this.logger.log(Level.FINEST, "Reading Interface configuration...");
		this.logger.log(Level.FINE, "Handling file: " + paramSourceFile.getName());
		this.techPack = paramString1;
		this.setType = paramString2;
		this.setName = paramString3;
		this.sourceFile = paramSourceFile;
		this.commonKeys = new HashMap<String, String>();
		this.generatedData = new HashMap<String, String>();
		this.objectMeasurementData = new HashMap<String, String>();
		this.objectVectorData = new LinkedHashMap<String, HashMap<String, String>>();
		this.tmpKeys = new HashMap<String, String>();
		this.vectorKeys = new HashSet<String>();
		this.data = new Vector<String>();
		this.blockCount = 0;
		this.isKeyRow = true;
		this.isVectorRow = false;
		this.isObjectHeaderRow = false;
		this.isRopHeaderRow = false;
		getInterfaceParameters();
		this.openFiles = new HashMap<String, MeasurementFile>();
		this.logger.log(Level.FINEST, "Creating SQSAsciiParser instance.");
		try {
			this.data = new Vector<String>();
			this.in = paramSourceFile.getFileInputStream();
			this.reader = new BufferedReader(new InputStreamReader(this.in));
			while ((this.result = this.reader.readLine()) != null) {
				int i = this.result.length();
				if (this.result.endsWith("SERVICE QUALITY STATISTICS RESULT")) {
					this.isRopHeaderRow = true;
					this.blockCount = 0;
					this.commonKeys = new HashMap<String, String>();
					continue;
				}
				if (this.result.equals("END")) {
					this.blockCount = -1;
					this.data.clear();
					writeData();
					this.commonKeys.clear();
					continue;
				}
				if (this.isObjectHeaderRow && this.result.length() > 0) {
					this.result = this.result.replaceAll("\\b\\s{2,}\\b", " ");
					this.object = this.result.split(" ")[0];
					this.event = this.result.split(" ")[1];
					this.generatedData.put("OBJECT", this.object);
					this.generatedData.put("EVENTS", this.event);
					this.generatedData.put("JVM_TIMEZONE", this.JVM_TIMEZONE);
					this.generatedData.put("FILENAME", paramSourceFile.getName());
					this.generatedData.put("DIRNAME", paramSourceFile.getDir());
					this.isObjectHeaderRow = false;
					this.data.clear();
					continue;
				}
				if (this.result.equals("OBJECT   EVENTS")) {
					writeData();
					this.objectMeasurementData = new HashMap<String, String>();
					this.objectVectorData = new LinkedHashMap<String, HashMap<String, String>>();
					this.generatedData = new HashMap<String, String>();
					this.isObjectHeaderRow = true;
					this.isRopHeaderRow = false;
					this.data.clear();
					continue;
				}
				if (this.isRopHeaderRow) {
					handleCommonKeys();
					continue;
				}
				if (i == 0) {
					this.blockCount++;
					handleMeasurement();
					this.isVectorRow = false;
					continue;
				}
				if (this.data != null)
					if (this.data.size() >= 2 && !this.isVectorRow) {
						this.data.remove(this.data.firstElement());
						this.data.add(this.result);
					} else {
						this.data.add(this.result);
					}
				if (isVectorKey()) {
					this.data.remove(this.data.firstElement());
					this.isVectorRow = true;
				}
			}
		} finally {
			this.reader.close();
			this.in.close();
			for (Map.Entry<String, MeasurementFile> entry : this.openFiles.entrySet()) {
				String str = (String) entry.getKey();
				MeasurementFile measurementFile = (MeasurementFile) entry.getValue();
				this.logger.log(Level.FINE, "Closing measurement file " + str);
				try {
					measurementFile.close();
				} catch (Exception exception) {
					this.logger.log(Level.SEVERE, "Unable to close MeasurementFile" + str, exception);
				}
			}
		}
		this.logger.log(Level.FINE, "Parse finished.");
	}

	public void run() {
		try {
			this.status = 2;
			SourceFile sourceFile = null;
			while ((sourceFile = this.main.nextSourceFile()) != null) {
				try {
					this.main.preParse(sourceFile);
					parse(sourceFile, this.techPack, this.setType, this.setName);
					this.main.postParse(sourceFile);
				} catch (Exception exception) {
					this.main.errorParse(exception, sourceFile);
				} finally {
					this.main.finallyParse(sourceFile);
				}
			}
		} catch (Exception exception) {
			this.logger.log(Level.SEVERE, "Parsing failed with exception!", exception);
		} finally {
			this.status = 3;
		}
	}

	public int status() {
		return this.status;
	}

	private MeasurementFile getMFile(String paramString) {
		try {
			MeasurementFile measurementFile = this.openFiles.get(paramString);
			if (measurementFile == null) {
				measurementFile = Main.createMeasurementFile(this.sourceFile, paramString, this.techPack, this.setType,
						this.setName, this.workerName, this.logger);
				this.openFiles.put(paramString, measurementFile);
			}
			return measurementFile;
		} catch (Exception exception) {
			this.logger.log(Level.SEVERE, "Unable to create measurement file", exception);
			return null;
		}
	}

	private void getInterfaceParameters() {
		this.tagIdField = this.sourceFile.getProperty("tagIdField", "OTYPE");
		this.vectorIndex = this.sourceFile.getProperty("vectorIndex", "DCVECTOR_INDEX");
		String str = this.sourceFile.getProperty("vectorKeys", "");
		if (!str.equals("")) {
			StringTokenizer stringTokenizer = new StringTokenizer(str, ",");
			while (stringTokenizer.hasMoreTokens()) {
				String str1 = stringTokenizer.nextToken().trim();
				this.vectorKeys.add(str1);
			}
		}
	}

	private void handleCommonKeys() {
		if (this.isKeyRow && this.result.length() > 0) {
			this.tmpKeys = getKeys();
			this.isKeyRow = false;
		} else if (this.result.length() > 0) {
			this.commonKeys.putAll(getValues(this.tmpKeys));
			this.isKeyRow = true;
			this.tmpKeys.clear();
		}
		this.logger.log(Level.FINEST, "Common keys: " + this.commonKeys);
	}

	private boolean isVectorKey() {
		String str = this.result.replaceAll("\\b\\s{2,}\\b", " ").trim();
		for (String str1 : this.vectorKeys) {
			if (str.startsWith(str1 + " "))
				return true;
		}
		return false;
	}

	private void writeData() {
		this.commonKeys.put("DATE_ID", this.commonKeys.get("DATE"));
		if (!this.objectMeasurementData.isEmpty() && !this.commonKeys.isEmpty()) {
			HashMap<Object, Object> hashMap = new HashMap<Object, Object>();
			hashMap.putAll(this.commonKeys);
			hashMap.putAll(this.generatedData);
			hashMap.putAll(this.objectMeasurementData);
			String str = (String) hashMap.get(this.tagIdField);
			MeasurementFile measurementFile = getMFile(str);
			if (measurementFile != null) {
				measurementFile.addData(hashMap);
				try {
					measurementFile.saveData();
				} catch (Exception exception) {
					this.logger.log(Level.SEVERE, "Unable to write MeasurementFile", exception);
				}
			}
		}
		if (!this.objectVectorData.isEmpty() && !this.commonKeys.isEmpty())
			for (Map.Entry<String, HashMap<String, String>> entry : this.objectVectorData.entrySet()) {
				HashMap<Object, Object> hashMap = new HashMap<Object, Object>();
				hashMap.putAll(this.commonKeys);
				hashMap.putAll(this.generatedData);
				hashMap.putAll((Map<?, ?>) entry.getValue());
				hashMap.put(this.vectorIndex, entry.getKey());
				String str = (String) hashMap.get(this.tagIdField);
				MeasurementFile measurementFile = getMFile(str + "_V");
				if (measurementFile != null) {
					measurementFile.addData(hashMap);
					try {
						measurementFile.saveData();
					} catch (Exception exception) {
						this.logger.log(Level.SEVERE, "Unable to write MeasurementVectorFile", exception);
					}
				}
			}
		this.objectMeasurementData.clear();
		this.objectVectorData.clear();
		this.generatedData.clear();
	}

	private void handleMeasurement() {
		String[] arrayOfString1 = new String[0];
		String[] arrayOfString2 = new String[0];
		int i = 0;
		int j = 0;
		String str1 = "";
		String str2 = "";
		byte b1 = 0;
		byte b2 = 0;
		for (String str3 : this.data) {
			String str4;
			String str5;
			HashMap<String, String> hashMap;
			switch (b1) {
			case 0:
				str4 = str3.replaceAll("\\b\\s{2,}\\b", " ").trim();
				arrayOfString1 = str4.split(" ");
				i = arrayOfString1.length;
				break;
			default:
				str5 = str3.replaceAll("\\b\\s{2,}\\b", " ").trim();
				hashMap = new HashMap<String, String>();
				arrayOfString2 = str5.split(" ");
				j = arrayOfString2.length;
				if (i != j) {
					HashMap<String, String> hashMap1 = scanMeasurementValues(str1, str2);
					byte b = 0;
					for (Map.Entry<String, String> entry : hashMap1.entrySet()) {
						arrayOfString1[b] = (String) entry.getKey();
						arrayOfString2[b] = (String) entry.getValue();
						b++;
					}
				}
				if (arrayOfString2.length >= arrayOfString1.length) {
					if (this.isVectorRow) {
						for (byte b3 = 0; b3 < arrayOfString1.length; b3++)
							hashMap.put(arrayOfString1[b3], arrayOfString2[b3]);
						this.objectVectorData.put(String.valueOf(b2), hashMap);
						b2++;
						break;
					}
					for (byte b = 0; b < arrayOfString1.length; b++)
						this.objectMeasurementData.put(arrayOfString1[b], arrayOfString2[b]);
				}
				break;
			}
			b1++;
			this.logger.log(Level.FINEST, "Obj Meas Data: " + this.objectMeasurementData);
		}
	}

	private HashMap<String, String> getKeys() {
		HashMap<String, String> hashMap = new HashMap<String, String>();
		byte b = 0;
		String str = "";
		StringBuffer stringBuffer = new StringBuffer();
		for (char c : this.result.toCharArray()) {
			if (!Character.isWhitespace(c)) {
				stringBuffer.append(c);
			} else {
				str = stringBuffer.toString();
				if (str.length() > 0) {
					hashMap.put(str, String.valueOf(b - str.length()));
					stringBuffer.setLength(0);
				}
			}
			b++;
		}
		str = stringBuffer.toString();
		hashMap.put(str, String.valueOf(b - str.length()));
		return hashMap;
	}

	private HashMap<String, String> scanMeasurementValues(String paramString1, String paramString2) {
		HashMap<Object, Object> hashMap = new HashMap<Object, Object>();
		int i = 0;
		String str = "";
		StringBuffer stringBuffer = new StringBuffer();
		for (char c : paramString1.toCharArray()) {
			if (!Character.isWhitespace(c)) {
				stringBuffer.append(c);
			} else {
				str = stringBuffer.toString();
				if (str.length() > 0) {
					hashMap.put(str, String.valueOf(i - str.length()));
					stringBuffer.setLength(0);
				}
			}
			i++;
		}
		str = stringBuffer.toString();
		hashMap.put(str, String.valueOf(i - str.length()));
		stringBuffer.setLength(0);
		for (Map.Entry<Object, Object> entry : hashMap.entrySet()) {
			str = (String) entry.getKey();
			i = Integer.parseInt((String) entry.getValue());
			String str1 = null;
			if (paramString2.length() > i) {
				String str2 = paramString2.substring(i);
				char[] arrayOfChar = str2.toCharArray();
				int j = arrayOfChar.length;
				byte b = 0;
				while (b < j) {
					char c = arrayOfChar[b];
					if (!Character.isWhitespace(c)) {
						stringBuffer.append(c);
						str1 = stringBuffer.toString();
						b++;
					}
				}
				hashMap.put(str, str1);
				stringBuffer.setLength(0);
				continue;
			}
			hashMap.put(str, null);
		}
		return (HashMap) hashMap;
	}

	private HashMap<String, String> getValues(HashMap<String, String> paramHashMap) {
		StringBuffer stringBuffer = new StringBuffer();
		for (Map.Entry<String, String> entry : paramHashMap.entrySet()) {
			String str1 = (String) entry.getKey();
			int i = Integer.parseInt((String) entry.getValue());
			String str2 = null;
			if (this.result.length() > i) {
				String str = this.result.substring(i);
				char[] arrayOfChar = str.toCharArray();
				int j = arrayOfChar.length;
				byte b = 0;
				while (b < j) {
					char c = arrayOfChar[b];
					if (!Character.isWhitespace(c)) {
						stringBuffer.append(c);
						str2 = stringBuffer.toString();
						b++;
					}
				}
				paramHashMap.put(str1, str2);
				stringBuffer.setLength(0);
				continue;
			}
			paramHashMap.put(str1, null);
		}
		return paramHashMap;
	}
}
