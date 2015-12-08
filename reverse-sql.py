'''
DATE: 12/08/2015
AUTHOR: ow
DESCRIPTION:
Given a file, which consists of a number of SQL Statements (and possibly other code), this script
does the following steps:
    1. Extract sql select statements from the file
    2. Extract tables and columns from the statements
    3. Exports table/column structure in a csv file

After the script ran, there are possibly several statements to manually check. Those are included
in the ouput csv file.

Note, that the following statements don't get evaluated:
    * INNER JOIN (only the table name gets extracted)
    * IF
    * Nested statements (SELECT (SELECT ..))
    * ORDER BY ..
'''
import sqlparse
from __builtin__ import isinstance
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML, Whitespace, Punctuation, Operator
import pyparsing as pypa
from xml.sax.saxutils import unescape #unescape some chars for file writing (e.g. &gt; to >)
import csv
import logging
#inputfile
inputFilePath = 'C:\\tmp\\sqlparser\\input\\UserCommands.fsx'
#output to CSV file
writeToCsv = True
outputFilePath = 'C:\\tmp\\sqlparser\\output.csv'
#set loglevel
logging.basicConfig(level=logging.FATAL)
logger = logging.getLogger(__name__)
#ignore strings in select statement, e.g. SELECT '<table>' ..
ignoreStringsInSelect = True
#
statementsToRecheck = set()

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
                    identifierString = str(identifier)
                    #we could scrap the alias here, but then we would not be able to correlate col names to tables afterwards
                    #if isinstance(identifier, sqlparse.sql.Identifier):
                        #identifierString = str(identifier.get_real_name())
                    yield identifierString.strip("()")#.get_name()
        elif isinstance(item, Identifier):
            yield str(item).strip("()")#.get_name()
        else:
            yield str(item).strip("()")

def extractTable(parsedStatement):
    from_seen = False
    innerJoinPresent = False
    innerJoinFound = False
    #search for inner join statement
    if not innerJoinFound:
        tokenIter = iter(parsedStatement.tokens)
        for item in tokenIter:
            if innerJoinPresent and not innerJoinFound:
                innerJoinFound = True
                yield next(tokenIter)
                break
            if str(item) == "INNER JOIN":
                innerJoinPresent = True
                logging.info("Found INNER JOIN - check statement: " + str(parsedStatement))
                statementsToRecheck.add(str(parsedStatement))
    for item in parsedStatement.tokens:
        if from_seen and item.ttype is not Whitespace:
            if item.ttype is Keyword or isinstance(item, sqlparse.sql.Where) and str(item):
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
    '''
    Wrapper function to extract the tables and columns for a given sql select statement.
    :param parsedStatement: string
    :return: returns dictionary from table to a list of columns.
    '''
    extracted = {}
    tableNames = set()
    columNames = set()
    logger.debug('_________')
    logger.debug('TABLE')
    for i in extractIdentifiers(extractTable(parsedStatement)):
        logger.debug('     ' + str(i))
        if str(i) != '':
            tableNames.add(str(i))

    logger.debug('COLS')
    for i in extractIdentifiers(extractColumns(parsedStatement)):
        logger.debug('     ' +  str(i))
        columNames.add(str(i))
    logger.debug('WHERE')
    for i in extractWhere(parsedStatement):
        logger.debug('     ' +  str(i).strip("()"))
        columNames.add(str(i).strip("()"))
    logger.debug('_________')

    extracted = orderResults(parsedStatement, tableNames, columNames)

    return extracted

def orderResults(parsedStatement, tables, cols):
    '''Function takes two input sets.
        Possible naming schemes:
            cols: a.ColumnName, b.Column2Name; tables: Table1 a, Table2 b
            cols: Table1.ColumnName, Table2.Column2Name; tables: Table1, Table2
        If len(tables) == 1, then all cols will be added to that one.
    :param tables: set of tables
    :param cols: set of columns
    :return: Dictionary of tables names to a set of column names.
    '''
    orderedResults = {}
    for table in tables:
        tableName = table
        columnsInTable = set()
        #case 1: Table1 a --> a.columnName
        if table.count(" ") > 0:
            tableName = table.split(" ")[0]
            aliasName = table.split(" ")[1]
            #special case: e.g. RollLoadEvents AS a
            if table.count(" ") > 1 and table.split(" ")[1] == 'AS':
                aliasName = table.split(" ")[2]
            for col in cols:
                if col.count(".") > 0:
                    if (aliasName == col.split(".")[0]): #a.columnName
                        columnsInTable.add(col.split(".")[1])
                else:
                    logger.error('Malformed statement for table ' + str(table) + ": " + str(col))
                    statementsToRecheck.add(str(parsedStatement))
        elif table.count(" ") == 0:
            if (len(tables) > 1): #multiple tables to choose from, so Table1.ColumnName, Table2.Column2Name; tables: Table1, Table2
                for col in cols:
                    if col.count(".") > 0:
                        if (table == col.split(".")[0]):
                            columnsInTable.add(col.split(".")[1])
                    else:
                        logger.error('Malformed statement for table ' + str(table) + ": " + str(col))
                        statementsToRecheck.add(str(parsedStatement))
            else: #only one table, normal case
                columnsInTable = cols

        #sanitize cols with aliases: 'Reel' AS Product
        itemsToChange = {}
        itemsToRemove = []
        for col in columnsInTable:
            wordCount = col.count(" ")
            if wordCount == 2:
                itemsToChange[col] = col.split(" ")[0]
            if ignoreStringsInSelect:#'Reel' AS Product
                if col.startswith("'") or col.isdigit():
                    if col in itemsToChange:
                        itemsToRemove.append(itemsToChange[col])
                    else:
                        itemsToRemove.append(col)

        for old,new in itemsToChange.iteritems():
            columnsInTable.remove(old)
            columnsInTable.add(new)
        for item in itemsToRemove:
            columnsInTable.remove(item)
        orderedResults[tableName] = columnsInTable

    return orderedResults

def extractSqlStatements(filepath):
    ''' Extracts all SELECT statements from a given file.
        The statements have to start with the string SELECT and end with a semicolon.
    :param filepath: absolute file path
    :return: list of sql select statements
    '''
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
            r",'/'-1"    : "",
            '.Start'    : '.MOD_START',
            '.End'      : '.MOD_END'
        }
        for origStr, repStr in replacements.iteritems():
            fileString = fileString.replace(origStr, repStr)
        i = 0
        for t,s,e in searchTerm.scanString(fileString):
            logger.debug("SELECT" + str(t.value[0]))
            sqlStatements.append("SELECT" + str(t.value[0]))
            i = i + 1
        logger.info("Number of statements: " + str(i))
        print("Number of statements: " + str(i))

        return sqlStatements

if __name__ == '__main__':
    if writeToCsv:
        outputFile = open(outputFilePath,'wb')
        csvWriter = csv.writer(outputFile)
        csvWriter.writerow(['Table', 'Columns'])

    extractedStatements = {}
    statements = extractSqlStatements(inputFilePath)
    for statement in statements:
        parsed = sqlparse.parse(statement)[0]
        logger.debug(statement)
        extraction = extractData(parsed)
        for entry in extraction:
            if entry in extractedStatements: #update existing entry
                extractedStatements[entry] = extractedStatements[entry].union(extraction[entry])
            else: #add new entry
                extractedStatements[entry] = extraction[entry]

    logger.debug('RESULTS')
    for table in extractedStatements:
        listToWrite = [table]
        logger.debug(table)
        for col in extractedStatements[str(table)]:
            logger.debug('         ' + str(col))
            listToWrite.append(col)
        if writeToCsv:
			csvWriter.writerow(listToWrite)

    if len(statementsToRecheck) > 0:
        if writeToCsv:
            csvWriter.writerow([])
            csvWriter.writerow(['RECHECK THE FOLLOWING ' + str(len(statementsToRecheck)) + ' STATEMENTS:'])
        print 'Check the following ' + str(len(statementsToRecheck)) + ' statements:'
        for statement in statementsToRecheck:
            print statement
            if writeToCsv:
                csvWriter.writerow([statement])

    if writeToCsv:
        outputFile.close()