import sqlparse
from __builtin__ import isinstance
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML, Whitespace, Punctuation, Operator
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
            yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True

def extractColumns(parsedStatement):
    select_seen = False
    distinct = False
    for item in parsedStatement.tokens:
        if select_seen and item.ttype is not Whitespace:
            if item.ttype is Keyword:
                if item.value.upper() == 'DISTINCT':
                    distinct = True
                else:
                    raise StopIteration
            # do something
            if not distinct:
                yield item
            distinct = False
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
    print '_________'
    print 'TABLE'
    for i in extractIdentifiers(extractTable(parsedStatement)):
        print '     ' + str(i)
    print 'COLS'
    for i in extractIdentifiers(extractColumns(parsedStatement)):
        print '     ' +  str(i)
    print 'WHERE'
    for i in extractWhere(parsedStatement):
        print '     ' +  str(i).strip("()")
    print '_________'

if __name__ == '__main__':
    with open('C:\\tmp\\sqlparser\\input\\input.txt') as file:
        for statement in file:
            parsed = sqlparse.parse(statement)[0]
            extractData(parsed)
