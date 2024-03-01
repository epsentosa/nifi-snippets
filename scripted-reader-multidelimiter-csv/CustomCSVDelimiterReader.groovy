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

class CSVCustomDelimiterReader implements RecordReader {

    private final BufferedReader bufferedReader
    private final Map<Integer, String> headerMap
    private final String delimiter
    private final SimpleRecordSchema simpleSchema


    public CSVCustomDelimiterReader(InputStream input) {
        bufferedReader = new BufferedReader(new InputStreamReader(input))
        String header = bufferedReader.readLine() //HEADER MUST IN FIRST LINE

        // define delimiter, careful on character that require escape character
        this.delimiter = "\\|\\|"
        this.headerMap = getColumnMap(header)
        this.simpleSchema = getSimpleSchema(headerMap)
    }

    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        try {
            final String line = bufferedReader.readLine()
            if (line == null) {
                return null
            }
            List<String> fields = line.split(delimiter)
            Map<String, Object> recordValues = [:]
            List<RecordField> recordFields = []
            headerMap.sort()

            headerMap.each {entry ->
                String rawValue = fields[entry.key]
                if (rawValue == "null" || rawValue == "NULL") {
                    rawValue = null
                }
                recordValues.put(entry.value, rawValue)
            }
            return new MapRecord(simpleSchema, recordValues)
        } catch (Exception e) {
            throw new MalformedRecordException("Error while getting next record", e)
        }
    }

    @Override
    public void close() throws IOException {
        bufferedReader.close()
    }

    @Override
    public RecordSchema getSchema() {
        return this.simpleSchema
    }

    private Map<Integer, String> getColumnMap(String header) {
        Map<Integer, String> headerMap = [:]
        List<String> headerLine = header.split(delimiter)
        for (int i = 0; i < headerLine.size(); i++) {
            headerMap[i] = headerLine[i]
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

class CSVCustomDelimiterReaderFactory extends AbstractControllerService implements RecordReaderFactory {

    public CSVCustomDelimiterReaderFactory() {
    }

    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream inputStream, final long inputLength, final ComponentLog componentLog) throws IOException {
        return new CSVCustomDelimiterReader(inputStream)
    }

}

reader = new CSVCustomDelimiterReaderFactory()
