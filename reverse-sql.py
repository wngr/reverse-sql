import sqlparse
from __builtin__ import isinstance
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML, Whitespace, Punctuation, Operator
import pyparsing as pypa
from xml.sax.saxutils import unescape #unescape some chars for file writing (e.g. &gt; to >)

import csv

def extractIdentifiers(tokenStream):
    for item in tokenStream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                #we can't use identifier.get_name() here, as names like 'dbo.QSV..' the dbo. gets truncated
                if isinstance(identifier, sqlparse.sql.Function):
                    for returnItem in extractFunctionArgument(identifier):
                        yield str(returnItem).strip("()")
                else:
                    #for returnItem in isolateArguments(identifier):
                        #yield str(returnItem).strip("()")
                    yield str(identifier).strip("()")#.get_name()
        elif isinstance(item, Identifier):
            yield str(item).strip("()")#.get_name()
        else:
            yield str(item).strip("()")

def extractTable(parsedStatement):
    from_seen = False
    for item in parsedStatement.tokens:
        if from_seen and item.ttype is not Whitespace:
            if item.ttype is Keyword or isinstance(item, sqlparse.sql.Where):
                raise StopIteration
            # do something
            if not item.ttype is sqlparse.tokens.Error:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True

def extractColumns(parsedStatement):
    select_seen = False
    dontReturnItem = False
    for item in parsedStatement.tokens:
        if select_seen and item.ttype is not Whitespace:
            if item.ttype is Keyword:
                if item.value.upper() == 'DISTINCT':
                    dontReturnItem = True
                elif item.value.upper() == 'AND':
                    dontReturnItem = True
                else:
                    raise StopIteration
            # do something
            if not dontReturnItem and item.ttype is not sqlparse.tokens.Punctuation and item.ttype is not sqlparse.tokens.Newline:
                yield item
            dontReturnItem = False
        elif item.ttype is DML and item.value.upper() == 'SELECT':
            select_seen = True

def extractWhere(parsedStatement):
    for item in parsedStatement.tokens:
        if isinstance(item, sqlparse.sql.Where):
            for subItem in item.tokens:
                if isinstance(subItem, sqlparse.sql.Comparison):
                    yield subItem.left
                elif isinstance(subItem, sqlparse.sql.Function):
                    for returnItem in extractFunctionArgument(subItem):
                        yield returnItem

def extractFunctionArgument(tokenStream):
    for item in tokenStream.tokens:
        if isinstance(item, sqlparse.sql.Parenthesis):
            #for subItem in item.tokens:
                #if subItem.ttype is not Whitespace and subItem.ttype is not Punctuation:
                    #yield subItem
            for returnItem in isolateArguments(item):
                yield returnItem

def isolateArguments(tokenStream):
    if tokenStream.is_group():
        for item in tokenStream.tokens:
            if item.ttype is not Whitespace and item.ttype is not Punctuation and item.ttype is not Operator:
                if item.is_group():#(len(item.tokens) > 1):
                    for returnItem in isolateArguments(item):
                        yield returnItem
                else:
                    yield item
    else:
        yield tokenStream

def extractData(parsedStatement):
    extracted = {}
    tableName = ''
    print '_________'
    print 'TABLE'
    i = 0
    for i in extractIdentifiers(extractTable(parsedStatement)):
        print '     ' + str(i)
        if str(i) != '':
            tableName = str(i)
            extracted[tableName] = set()
        if (i>1):
            print 'TODO'
    if tableName != '':
        print 'COLS'
        for i in extractIdentifiers(extractColumns(parsedStatement)):
            print '     ' +  str(i)
            extracted[tableName].add(str(i))
        print 'WHERE'
        for i in extractWhere(parsedStatement):
            print '     ' +  str(i).strip("()")
            extracted[tableName].add(str(i).strip("()"))
        print '_________'

    return extracted

def extractSqlStatements(filepath):
    startTerm = "SELECT"
    endTerm = ";"

    searchTerm = startTerm + pypa.OneOrMore(~pypa.Literal(endTerm) + pypa.CharsNotIn(";")).setResultsName("value") + endTerm
    sqlStatements = []
    with open(filepath) as file:
        fileString = unescape(file.read())
        replacements = {
            '",'        : '',
            ',"'        : '',
            '("'        : '',
            ')'         : '',
            '\\'        : '',
            "+'-'+"     : ",",
            "':'+"      : ",",
            ' length '  :' MOD_Length ',
            ' type '    :' MOD_TYPE ',
            'iif'       : '',
            r",'/'-1"    : ""
        }
        for origStr, repStr in replacements.iteritems():
            fileString = fileString.replace(origStr, repStr)
        #fileString = fileString.replace(r'"', '',)
        #fileString = fileString.replace(r"'", "")
        #fileString = fileString.replace(r"<", "")
        #fileString = fileString.replace(r">", "")
        i = 0
        for t,s,e in searchTerm.scanString(fileString):
            print "SELECT" + str(t.value[0])
            sqlStatements.append("SELECT" + str(t.value[0]))
            i = i + 1
        print i

        return sqlStatements

if __name__ == '__main__':
    writeToCsv = True
    if writeToCsv:
        outputFile = open('C:\\tmp\\sqlparser\\output.csv','wb')
        csvWriter = csv.writer(outputFile)

    extractedStatements = {}
    statements = extractSqlStatements('C:\\tmp\\sqlparser\\input\\UserCommands.fsx')#someCmds.fsx')
    for statement in statements:
        parsed = sqlparse.parse(statement)[0]
        print(statement)
        extraction = extractData(parsed)
        extractedStatements.update(extraction)

    print 'RESULTS'
    for table in extractedStatements:
        listToWrite = [table]
        print table
        for col in extractedStatements[str(table)]:
            print '         ' + str(col)
            listToWrite.append(col)
        if writeToCsv:
			csvWriter.writerow(listToWrite)


    if writeToCsv:
        outputFile.close()