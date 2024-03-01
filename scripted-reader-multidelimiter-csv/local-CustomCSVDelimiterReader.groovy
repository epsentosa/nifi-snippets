def test_data = '''id||first_name||last_name||email||gender||ip_address
1||Albert||Mount||amount0@nature.com||Male||57.131.231.255
2||Skipper||O'Neil||soneil1@utexas.edu||Male||221.150.232.163
3||Clive||Shinner||cshinner2@webnode.com||Male||62.75.110.24
4||Flynn||Clother||fclother3@fema.gov||Male||180.1.32.206
5||Odey||Stubbings||ostubbings4@4shared.com||Male||111.20.121.192
6||Marylynne||Gillivrie||mgillivrie5@cocolog-nifty.com||Agender||79.88.39.190
7||Howard||Vanshin||hvanshin6@g.co||Male||152.78.112.139
8||Stillman||Gillyatt||||Male||75.19.216.24
9||Catlee||Danilowicz||||Female||8.75.200.96
10||Rhett||Lockyear||||Non-binary||91.94.138.7
'''

Map<String, List<String>> splitHeaderAndContent(String text) {
    String header
    List<String> content
    Map<String, List<String>> splittedContent = [:]
    List<String> textList = text.split('\\n')
    header = textList[0]
    content = textList[1..textList.size()-1]
    splittedContent.put(header, content)
    return splittedContent
}

Map<Integer, String> getColumnMap(String header) {
    Map<Integer, String> headerMap = [:]
    List<String> headerLine = header.tokenize('||')
    for (int i = 0; i < headerLine.size(); i++) {
        headerMap[i] = headerLine[i]
    }
    return headerMap
}

Map<String, Object> parse(String line, Map<Integer, String> headerMap) {
    if (line == null) {
        return null
    }

    List<String> fields = line.split("\\|\\|")
    Map<String, Object> recordValues = [:]
    headerMap.sort()
    headerMap.each {entry ->
        String rawValue = fields[entry.key]
        if (rawValue == "null" || rawValue == "NULL") {
            rawValue = ""
        }
        recordValues.put(entry.value, rawValue)
    }
    return recordValues
}

def mapSplit = splitHeaderAndContent(test_data)
def header = mapSplit.keySet()[0]
def content = mapSplit.get(header)
def headerMap = getColumnMap(header)

content.each {line ->
    println(parse(line, headerMap))
}
