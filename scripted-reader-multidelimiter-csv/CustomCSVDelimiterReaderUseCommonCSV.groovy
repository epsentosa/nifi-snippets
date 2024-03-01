import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import org.apache.commons.csv.CSVFormat
import org.apache.commons.io.input.BOMInputStream
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.RecordReader
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.serialization.MalformedRecordException
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.RecordSchema
import org.apache.nifi.serialization.record.RecordFieldType

class CSVMultiDelimiterReader implements RecordReader {

    private final CSVParser csvParser
    private final Map<Integer, String> headerMap
    private final String delimiter
    private final SimpleRecordSchema simpleSchema

    public CSVMultiDelimiterReader(InputStream in) {
        final InputStream bomInputStream = new BOMInputStream(in)
        final Reader reader = new InputStreamReader(bomInputStream, "UTF-8")

        this.delimiter = "||"

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setDelimiter(delimiter).setHeader().build()
        this.csvParser = new CSVParser(reader, csvFormat)

        this.headerMap = getColumnMap()
        this.simpleSchema = getSimpleSchema(headerMap)
    }

    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        try {
            final int numFieldNames = headerMap.size()

            for (final CSVRecord record: csvParser) {
                final Map<String, Object> recordValues = [:]
                for (int i = 0; i < record.size(); i++) {
                    if (i >= numFieldNames) {
                        continue
                    } else {
                        String rawValue = record.get(i)
                        if (rawValue == "null" || rawValue == "NULL") {
                            rawValue = null
                        }
                        recordValues.put(headerMap[i], rawValue)
                    }
                }
                return new MapRecord(simpleSchema, recordValues)
            }
        } catch (Exception e) {
            throw new MalformedRecordException("Error while getting next record", e)
        }
        return null
    }

    @Override
    public void close() throws IOException {
        csvParser.close()
    }

    @Override
    public RecordSchema getSchema() {
        return this.simpleSchema
    }

    private Map<Integer, String> getColumnMap() {
        final Map<Integer, String> headerMap = new HashMap<>()
        for (entry: csvParser.getHeaderMap().entrySet()) {
            headerMap.put(entry.getValue(), entry.getKey())
        }

        return headerMap
    }
    private SimpleRecordSchema getSimpleSchema(Map<Integer, String> headerMap) {
        List<RecordField> recordFields = []
        headerMap.sort()
        headerMap.each {entry ->
            recordFields.add(new RecordField(entry.value, RecordFieldType.STRING.getDataType()))
        }
        SimpleRecordSchema schema = new SimpleRecordSchema(recordFields)
        return schema
    }

    // custom function might useful handling anomaly value
    private boolean isValidId(String id) {
        try {
            id.toInteger()
            return true
        } catch (Exception e) {
            return false
        }
    }

    private boolean isValidDate(String dateString) {
        try {
            def parsedDate = Date.parse("yyyy-MM-dd", dateString)
            return true
        } catch (Exception e) {
            return false
        }
    }

    private boolean isValidDateTime(String dateTimeString) {
        try {
            def parsedDate = Date.parse("yyy-MM-dd HH:mm:ss", dateTimeString)
            return true
        } catch (Exception e) {
            return false
        }
    }
}

class CSVMultiDelimiterReaderFactory extends AbstractControllerService implements RecordReaderFactory {

    public CSVMultiDelimiterReaderFactory() {
    }

    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream inputStream, final long inputLength, final ComponentLog componentLog) throws IOException {
        return new CSVMultiDelimiterReader(inputStream)
    }

}

reader = new CSVMultiDelimiterReaderFactory()
