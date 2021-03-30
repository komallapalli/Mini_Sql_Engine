import sys
import csv
import re
import unicodedata
from sets import Set

availableTables = []
tableAppearingOrders = {}   # Maintains order in which tables appear. For ex select * from table2,table1 -> tableAppearingOrders['table2'] = 0, 'table1' = 1
headingNamesInTables = {}

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
    try:
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    return False


def Sum(joinedList,A,tableName):
    tableNumHere = tableAppearingOrders[tableName]
    sum1=0
    for i in range(0,len(joinedList)):
        sum1 += int(A[ joinedList[i][tableNumHere] ])
    return sum1

def Max(joinedList,A,tableName):
    tableNumHere = tableAppearingOrders[tableName]
    max1 = int(A[ joinedList[0][tableNumHere] ])
    for i in range(1,len(joinedList)):
        max1 = max( max1,int(A[joinedList[i][tableNumHere]] ))
    return max1

def Min(joinedList,A,tableName):
    tableNumHere = tableAppearingOrders[tableName]
    min1 = int(A[ joinedList[0][tableNumHere] ])
    for i in range(1,len(joinedList)):
        min1 = min( min1,int(A[joinedList[i][tableNumHere]] ))
    return min1

def Avg(joinedList,A,tableName):
    avg=(float(Sum(joinedList,A,tableName)))/len(joinedList)
    return avg

# Creating Classes Dynamically Based on Metadata
def CreateClasses():
    metadataFile = open('files/metadata.txt','r')
    errorFlag=0;        # 0 means no error found till now
    typeclasses = {}   # typeclasses['table1'] stores the class defn of table1
    start_flag = 0    # flag=0 , Means that we are at starting of file
    variableDict = {}   # Stores the variables initialization in class
    lineCount=0        # Stores the line number which we are at present in each table schema
    headingsList = []   # Stores the Column names of each table
    tableName = ''
    for line in metadataFile:
        lineCount=lineCount+1
        line=line.strip(' \t\n\r')   #removing last character which is a newline character
        if (start_flag==0):
            if (line=="<begin_table>"):
                start_flag=1
            else:
                errorFlag=1
                print "Invalid Metadata, Each Table Schema should begin with <begin_table>"
                break
        else :            #Start_flag = 1
            if (line=="<end_table>"):
                variableDict['headingsList'] = headingsList
                variableDict['numOfRecords'] = 0
                typeclasses[tableName] = type(tableName,(object,),variableDict)
                availableTables.append(tableName)
                headingNamesInTables[tableName] = headingsList
                start_flag=0
                tableName=''
                lineCount=0
                headingsList = []
                variableDict={}
            elif (line=="<begin_table>"):
                print "Invalid Metadata, Opening Tag Found in Place of Closing Tag"
                errorFlag=1
                break
            elif lineCount==2:
                tableName=line
            else:
                variableDict[line] = []
                headingsList.append(line)
    if start_flag==1:
        errorFlag=1
        print "Invalid Metadata, No closing Tag (<end_table>) Found"
        return
    else:
        return typeclasses

def parseQuery(query):
    conditionString = ''
    tableString = ''
    columnString = ''
    query = query.split()
    str = ''.join(query)        # This is the whole query string without any spaces in between
    temp1 = str.split('where')
    str1 = temp1[0]
    if (len(temp1)==2):
        conditionString = temp1[1]
    temp2 = str1.split('from')
    str2 = temp2[0]
    if (len(temp2)==2):
        tableString = temp2[1]
    temp3 = str2.split('select')
    if (len(temp3)==2):
        columnString = temp3[1]
    return [columnString,tableString,conditionString]

def checkTables(tableList):
    AllTablesPresentFlag=1  # Initially assume that all are present
    for i in range(0,len(tableList)):
        foundFlag=0     #tableList[i] not found
        for j in range(0,len(availableTables)):
            if (tableList[i]==availableTables[j]):
                foundFlag=1
                break
        if foundFlag==0:
            AllTablesPresentFlag=0
            break
    return AllTablesPresentFlag

def findColumnPresentTable(columnName,tableList):     #Returns all tables which contain the column name
    dotPresentPattern = re.compile('^[a-zA-Z][a-zA-Z0-9]*[.][a-zA-Z][a-zA-Z0-9]*$')
    normalPattern = re.compile('^[a-zA-Z][a-zA-Z0-9]*$')
    if (dotPresentPattern.match(columnName)):       # If so , dot is present in the columnName
        tableNamePart = columnName.split('.')[0]
        colNamePart = columnName.split('.')[1]
        tableFoundFlag=0
        colFoundFlag=0
        for i in range(0,len(tableList)):
            if (tableList[i]==tableNamePart):
                tableFoundFlag=1
                break
        if (tableFoundFlag==1):
            for i in range(0,len(headingNamesInTables[tableNamePart])):
                if (headingNamesInTables[tableNamePart][i]==colNamePart):
                    colFoundFlag=1
                    break
        if (tableFoundFlag==1) and (colFoundFlag==1) :
            return [tableNamePart]
        else:
            return []
    elif (normalPattern.match(columnName)):
        foundList = []
        for i in range(0,len(tableList)):
            for j in range(0,len(headingNamesInTables[tableList[i]])):
                if columnName==headingNamesInTables[tableList[i]][j]:
                    foundList.append(tableList[i])
                    break
        return foundList
    else:
        return []

def checkColumns(columnList,tableList):
    AggregateQueryPattern = re.compile('^[a-zA-Z][a-zA-Z0-9]*[(][a-zA-Z][a-zA-Z0-9]*[.][a-zA-Z][a-zA-Z0-9]*[)]$|^[a-zA-Z][a-zA-Z0-9]*[(][a-zA-Z][a-zA-Z0-9]*[)]$')
    validFlag=0     # 0 means Input Columns not valid, 1 means valid
    numDistinctColumnsInput=0   # All or none of input columns must be dist
    numAggregateColumnsInput=0   # All or none of input columns must be Aggregate
    if len(columnList)==1 and columnList[0]=='*':
        validFlag=1
        return 1
    else:
        for i in range(0,len(columnList)):
            if (AggregateQueryPattern.match(columnList[i])):  # This means that the column is an AggregateQuery
                temp1=columnList[i].split('(')
                aggFunc = temp1[0]
                if(aggFunc=='dist'):
                    numDistinctColumnsInput+=1
                elif (aggFunc=='sum') or (aggFunc=='min') or (aggFunc=='max') or (aggFunc=='avg'):
                    numAggregateColumnsInput+=1
                else:
                    validFlag=0
                    print "Error : Unknown Aggregate Function , "+aggFunc
                    return 0
                temp2=temp1[1]
                col=temp2.split(')')[0]
                foundTablesList = findColumnPresentTable(col,tableList)
                if len(foundTablesList)==0:
                    validFlag=0
                    print "Error : Unknown Column "+col+" in the tables Mentioned as Input"
                    return 0
                if (len(foundTablesList)==1):
                    validFlag=1
                else :
                    validFlag=0
                    print "Error : Column "+col+" in the field list is ambiguous"
                    return 0
            elif (len(columnList[i].split('.'))==1):  #Input Column Doesn't have '.'
                occurrenceCount=0       # stores the number of times this column occurs in different tables
                for j in range(0,len(tableList)):   # Iterating through each table
                    foundTable=0        # 0 means Not found in table
                    columnsPresent = headingNamesInTables[tableList[j]]     # This List contains all the Headings in this table
                    for k in range(0,len(columnsPresent)):              # Iterating over all the headings in the present table
                        if (columnsPresent[k]==columnList[i]):
                            foundTable=1
                            break
                    if foundTable==1:
                        occurrenceCount+=1
                if (occurrenceCount==0):
                    print "Error : Unknown Column "+columnList[i]+" in the tables Mentioned as Input"
                    validFlag=0
                    return 0
                elif (occurrenceCount==1):
                    validFlag=1
                elif (occurrenceCount>1):
                    print occurrenceCount
                    print "Error : Column "+columnList[i]+" in field list is Ambigous"
                    validFlag=0
                    return 0
            elif (len(columnList[i].split('.'))==2):
                temp = columnList[i].split('.')
                tablePart = temp[0]
                columnPart = temp[1]
                tableFound=0        # 0 means that still table not found
                columnFound=0       # 0 means that still column not found
                for p in range(0,len(availableTables)):
                    if availableTables[p]==tablePart:
                        tableFound=1
                        break
                if tableFound==1:
                    dicti =  headingNamesInTables[tablePart]
                    for p in range(0,len(dicti)):
                        if dicti[p]==columnPart:
                            columnFound=1
                            break
                if (tableFound==1) and (columnFound==1):
                    validFlag=1
                else:
                    print "Error : Unknown Column "+columnList[i]+" in the tables Mentioned Input"
                    validFlag=0
                    return 0
            else:
                print "Error : Unknown Column "+columnList[i]+" in the tables Mentioned as Input"
                validFlag=0
                return 0
        if validFlag==1:
            if (numAggregateColumnsInput==len(columnList)):
                return 1
            elif numAggregateColumnsInput==0:
                if (numDistinctColumnsInput==len(columnList)):
                    return 1
                elif numDistinctColumnsInput==0:
                    return 1
                else:
                    print "Error : Either All or None of Distinct Functions"
                    validFlag=0
                    return 0
            else:
                print "Error : Either All or None of Aggregate Functions"
                validFlag=0
                return 0

def queryCheckSemantics(columnList,tableList):
    # There definitely will be columnList, tableList but not conditionList
    TablesValidflag = checkTables(tableList)   # flag 0 means input tables not present, 1 means all are present
    if TablesValidflag==0:
        print "Error : Input Tables Not Present or Unknown Table, Please Recheck"
        return 0
    else:
        ColumnsValidFlag = checkColumns(columnList,tableList)
        if ColumnsValidFlag==1:
            return 1
        else:
            return 0

def createTableObjects(tableList,ClassDefinitions):
    tableObjects = {}
    for i in range(0,len(tableList)):
        tableObjects[tableList[i]] = ClassDefinitions[tableList[i]]()
    return tableObjects

def enterDataInObjects(tableObjects,tableList):
    for i in range(0,len(tableList)):
        present_table_object = tableObjects[tableList[i]]
        headingsPresentInObject = present_table_object.headingsList
        csvfile = open('files/'+tableList[i]+'.csv','rb')
        records_num=0
        for line in csvfile:
            formattedLine = line.strip(' \n\r')
            columnRecords = formattedLine.split(',')
            for j in range(0,len(columnRecords)):
                attr = headingsPresentInObject[j]
                getattr(present_table_object,attr).append(columnRecords[j])
            records_num += 1
        present_table_object.numOfRecords = records_num
    return tableObjects

def joinFunc(tabIndexList,n):   # Returns the cartesian product of tables numbered from 0 to n
    if n==0:
        return tabIndexList[0]
    cartProduct = joinFunc(tabIndexList,n-1)
    l = len(cartProduct)
    newlist = []
    for i in range(0,l):
        for j in range(0,len(tabIndexList[n])):
            temp_list = cartProduct[i][:]
            temp_list.append(tabIndexList[n][j][0])
            newlist.append(temp_list)
    return newlist

def convertHeading(columnList,tableList,ClassDefinitions):
    AggregateQueryPattern = re.compile('^[a-zA-Z][a-zA-Z0-9]*[(][a-zA-Z][a-zA-Z0-9]*[.][a-zA-Z][a-zA-Z0-9]*[)]$|^[a-zA-Z][a-zA-Z0-9]*[(][a-zA-Z][a-zA-Z0-9]*[)]$')
    dotPresentPattern = re.compile('^[a-zA-Z][a-zA-Z0-9]*[.][a-zA-Z][a-zA-Z0-9]*$')
    normalPattern = re.compile('^[a-zA-Z][a-zA-Z0-9]*$')
    convertedList = []
    if len(columnList)==1 and columnList[0]=='*':
        for i in range(0,len(tableList)):
            tableObj = ClassDefinitions[tableList[i]]
            arr = tableObj.headingsList
            for j in range(0,len(arr)):
                convertedList.append(tableList[i]+'.'+arr[j])
        return convertedList
    for i in range(0,len(columnList)):
        if (AggregateQueryPattern.match(columnList[i])):
            convertedList.append(columnList[i])
        elif (dotPresentPattern.match(columnList[i])):
            convertedList.append(columnList[i])
        elif (normalPattern.match(columnList[i])):
            tableName = findColumnPresentTable(columnList[i],tableList)[0]
            convertedList.append(tableName+'.'+columnList[i])
        else:
            print "error over here 1"
    return convertedList

def extractColsOperators(condition):
    operatorList = ['>=','<=','=','<','>']
    crctOperatorFlag=0
    for i in range(0,len(operatorList)):
        if (len(condition.split(operatorList[i]))==2):
            crctOperatorFlag=1
            col1 = condition.split(operatorList[i])[0]
            col2 = condition.split(operatorList[i])[1]
            operator = operatorList[i]
            if operator == '=':
                operator = '=='
            return [col1,operator,col2]
    if crctOperatorFlag==0:
        print "Unexpected Operator in Where Condition"
        sys.exit()

def whereConditionErrorChecking(col11,col21,col12,col22,tableList):
    temp = [col11,col21,col12,col22]
    for i in range(0,len(temp)):        # Error Checking Module
        if(is_number(temp[i])):
            continue
        if ( len(findColumnPresentTable(temp[i],tableList)) == 0 ):
            print "Error : Unknown Column "+temp[i]+" in the tables Mentioned as Input"
            sys.exit()
        elif ( len(findColumnPresentTable(temp[i],tableList)) == 2 ):
            print "Error : Column "+temp[i]+" in the field list is ambiguous"
            sys.exit()

def whereConditionErrorChecking1(col1,col2,tableList):  # this function to handle errors when no AND, OR operator pesent
    temp = [col1,col2]
    for i in range(0,len(temp)):        # Error Checking Module
        if(is_number(temp[i])):
            continue
        if ( len(findColumnPresentTable(temp[i],tableList)) == 0 ):
            print "Error : Unknown Column "+temp[i]+" in the tables Mentioned as Input"
            sys.exit()
        elif ( len(findColumnPresentTable(temp[i],tableList)) == 2 ):
            print "Error : Column "+temp[i]+" in the field list is ambiguous"
            sys.exit()

def convertTableColFormat(col,tableList):
    if is_number(col):
        return col
    dotPresentPattern = re.compile('^[a-zA-Z][a-zA-Z0-9]*[.][a-zA-Z][a-zA-Z0-9]*$')
    if dotPresentPattern.match(col):
        return col
    else :
        tableNameHere = findColumnPresentTable(col,tableList)[0]
        return tableNameHere+'.'+col

def evaluate2Condn(joinedList,logicalOperator,tableList,conditionString,tableObjects):
    reqList = []
    condn1 = conditionString.split(logicalOperator)[0]
    condn2 = conditionString.split(logicalOperator)[1]
    [col11,operator1,col21] = extractColsOperators(condn1) # Col21 means col2 of first expression in where condition
    [col12,operator2,col22] = extractColsOperators(condn2)
    whereConditionErrorChecking(col11,col21,col12,col22,tableList)    # Error Filter in Where Condition
    temp = [col11,col21,col12,col22]
    for p in range(0,len(temp)):
        temp[p] = convertTableColFormat(temp[p],tableList)
    for i in range(0,len(joinedList)):
        evalString = ""
        for j in range(0,len(temp)):
            if is_number(str(temp[j])):
                evalString += temp[j]
            else :
                tableNameHere = temp[j].split('.')[0]
                pureColName = temp[j].split('.')[1]
                tableNumberHere = tableAppearingOrders[tableNameHere]
                evalString += getattr(tableObjects[tableNameHere],pureColName)[joinedList[i][tableNumberHere]]
            if j==0:
                evalString += operator1
            elif j==1:
                evalString += ' '+logicalOperator+' '
            elif j==2:
                evalString += operator2
        if (eval(evalString)):
            reqList.append(joinedList[i][:])
    return reqList

def evaluate1Condn(joinedList,tableList,conditionString,tableObjects):
    reqList = []
    condn = conditionString
    [col1,operator,col2] = extractColsOperators(condn)
    whereConditionErrorChecking1(col1,col2,tableList)
    temp = [col1,col2]
    for p in range(0,len(temp)):
        temp[p] = convertTableColFormat(temp[p],tableList)
    for i in range(0,len(joinedList)):
        evalString = ""
        for j in range(0,len(temp)):
            if is_number(str(temp[j])):
                evalString += temp[j]
            else :
                tableNameHere = temp[j].split('.')[0]
                pureColName = temp[j].split('.')[1]
                tableNumberHere = tableAppearingOrders[tableNameHere]
                evalString += getattr(tableObjects[tableNameHere],pureColName)[joinedList[i][tableNumberHere]]
            if j==0:
                evalString += operator
        if (eval(evalString)):
            reqList.append(joinedList[i][:])
    return reqList

def conditionFilter(joinedList,conditionString,tableObjects,tableList):
    reqList = []
    if (len(conditionString.split('and'))==2):  # Means 'AND' is present in the condition
        reqList = evaluate2Condn(joinedList,'and',tableList,conditionString,tableObjects)
    elif (len(conditionString.split('or'))==2):     # Means 'OR' is present in the condition
        reqList = evaluate2Condn(joinedList,'or',tableList,conditionString,tableObjects)
    else:       # Means none of 'AND' , 'OR' is present
        if (conditionString==''):
            reqList = joinedList[:]
        else:
            reqList = evaluate1Condn(joinedList,tableList,conditionString,tableObjects)
    return reqList

def getPureCol(col):
    dotPresentPattern = re.compile('^[a-zA-Z][a-zA-Z0-9]*[.][a-zA-Z][a-zA-Z0-9]*$')
    if(dotPresentPattern.match(col)):
        pureColumn = col.split('.')[1]
    else :
        pureColumn = col
    return pureColumn

def projectJoin(convertedHeadingsList,conditionString,tableList):
    newlist = []
    if (len(conditionString.split('and'))==2):      # Means 'AND' is present in the condition
        return convertedHeadingsList
    elif (len(conditionString.split('or'))==2):     # Means 'OR' is present in the condition
        return convertedHeadingsList
    else:                                           # Means none of 'AND' , 'OR' is present
        if (conditionString==''):
            return convertedHeadingsList
        else:
            condn = conditionString
            [col1,operator,col2] = extractColsOperators(condn)
            if (is_number(col1)):
                print "Error: Invalid Syntax, Cannot Be Executed"
                sys.exit()
            elif (is_number(col2)):
                return convertedHeadingsList
            else:
                tab1 = findColumnPresentTable(col1,tableList)[0]
                tab2 = findColumnPresentTable(col2,tableList)[0]
                purecol1 = getPureCol(col1)
                purecol2 = getPureCol(col2)
                if ((tab1+'.'+purecol1) != (tab2+'.'+purecol2)) and (tab2+'.'+purecol2 in convertedHeadingsList):
                    convertedHeadingsList.remove(tab2+'.'+purecol2)
                return convertedHeadingsList

def queryExecutionModule(ClassDefinitions,columnList,tableList,conditionList,conditionString):
    AggregateQueryPattern = re.compile('^[a-zA-Z][a-zA-Z0-9]*[(][a-zA-Z][a-zA-Z0-9]*[.][a-zA-Z][a-zA-Z0-9]*[)]$|^[a-zA-Z][a-zA-Z0-9]*[(][a-zA-Z][a-zA-Z0-9]*[)]$')
    dotPresentPattern = re.compile('^[a-zA-Z][a-zA-Z0-9]*[.][a-zA-Z][a-zA-Z0-9]*$')
    normalPattern = re.compile('^[a-zA-Z][a-zA-Z0-9]*$')
    IdentifyDISTpattern = re.compile('^[dD][iI][sS][tT]$')
    # Create Objects of tables classes
    tableObjects = createTableObjects(tableList,ClassDefinitions)
    # Enter Data into the table classes
    tableObjects = enterDataInObjects(tableObjects,tableList)
    # Create tabIndexList, for example tabIndexList = [[[0], [1], [2], [3], [4], [5], [6], [7], [8], [9]], [[0], [1], [2], [3], [4], [5], [6], [7], [8], [9]]] to do join
    tabIndexList = []
    for i in range(0,len(tableList)):
        tabIndexList.append([])
        for j in range(0,tableObjects[tableList[i]].numOfRecords):
            tabIndexList[i].append([j])
    # Execute Join operation
    joinedList = joinFunc(tabIndexList,len(tabIndexList)-1)
    # Apply the where conditions on the above joined list
    joinedList = conditionFilter(joinedList,conditionString,tableObjects,tableList)
    # Convert all Input Columns into 'table.column' format or 'Aggregate(table.column)'
    convertedHeadingsList = convertHeading(columnList,tableList,ClassDefinitions)
    # Remove the unnecessary columns among join based columns (Remove B in A=B)
    print convertedHeadingsList
    convertedHeadingsList = projectJoin(convertedHeadingsList,conditionString,tableList)
    headingStr = ""
    for q in range(0,len(convertedHeadingsList)):
        headingStr+=(convertedHeadingsList[q] + ',')
    headingStr = headingStr[:-1]
    print headingStr
    if (dotPresentPattern.match(convertedHeadingsList[0])):     # Means all are present in table.column format
        for i in range(0,len(joinedList)):           # Iterating over every possible permutation
            rowString = ""
            for j in range(0,len(convertedHeadingsList)):
                tableName = convertedHeadingsList[j].split('.')[0]
                colName = convertedHeadingsList[j].split('.')[1]
                tableObj = tableObjects[tableName]
                colDataList = getattr(tableObj,colName)
                tableNum = tableAppearingOrders[tableName]
                rowString += str(colDataList[joinedList[i][tableNum]])+','
            rowString = rowString[:-1]      # Removing the last comma
            print rowString

    elif (AggregateQueryPattern.match(convertedHeadingsList[0])):   # Means all are Aggregate Query or Distinct Query
        if(convertedHeadingsList[0].split('(')[0]=='dist'):     #All are DIST Query
            distinctRecordSet = Set([])
            for i in range(0,len(joinedList)):
                rowString = ""
                for j in range(0,len(convertedHeadingsList)):
                    aggQuery = convertedHeadingsList[j]
                    temp = aggQuery.split('(')[1]
                    column = temp.split(')')[0]         # part inside brackets
                    if(dotPresentPattern.match(column)):
                        pureColumn = column.split('.')[1]
                    else :
                        pureColumn = column
                    tableName = findColumnPresentTable(column,tableList)[0]      # tells in which table the non ambiguous column resides
                    tableNum = tableAppearingOrders[tableName]
                    correspondingTableObj = tableObjects[tableName]
                    colDataList = getattr(correspondingTableObj,pureColumn)
                    rowString += str(colDataList[joinedList[i][tableNum]])+','
                rowString = rowString[:-1]
                if not (rowString in distinctRecordSet):
                    print rowString
                    distinctRecordSet.add(rowString)

        else:   # All are Aggregate queries
            rowString = ""
            for i in range(0,len(convertedHeadingsList)):
                aggQuery = convertedHeadingsList[i]
                aggFunction = aggQuery.split('(')[0]    # function name
                temp = aggQuery.split('(')[1]
                column = temp.split(')')[0]         # part inside brackets
                if(dotPresentPattern.match(column)):
                    pureColumn = column.split('.')[1]
                else :
                    pureColumn = column
                tableName = findColumnPresentTable(column,tableList)[0]      # tells in which table the non ambiguous column resides
                correspondingTableObj = tableObjects[tableName]
                if (aggFunction=='sum'):
                    rowString += str( Sum(joinedList,getattr(correspondingTableObj,pureColumn),tableName) ) + ','
                elif (aggFunction=='max'):
                    rowString += str( Max(joinedList,getattr(correspondingTableObj,pureColumn),tableName) ) + ','
                    # rowString += str( correspondingTableObj.Max(getattr(correspondingTableObj,pureColumn)) ) + ','
                elif (aggFunction=='min'):
                    rowString += str( Min(joinedList,getattr(correspondingTableObj,pureColumn),tableName) ) + ','
                    # rowString += str( correspondingTableObj.Min(getattr(correspondingTableObj,pureColumn)) ) + ','
                elif (aggFunction=='avg'):
                    rowString += str( Avg(joinedList,getattr(correspondingTableObj,pureColumn),tableName) ) + ','
                    # rowString += str( correspondingTableObj.Avg(getattr(correspondingTableObj,pureColumn)) ) + ','
                else :  # Shouldn't come to this point , overcoming all the error filters
                    print "Something wrong here 105"
            rowString = rowString[:-1]
            print rowString
    else:       # Shouldn't come to this point , overcoming all the error filters
        print "Uh oH something is wrong 1"


def main():
    ClassDefinitions = CreateClasses()    # ClassDefinitions['table1']() gives the class definition of class 1
    if ClassDefinitions==None:
        return
    [columnString,tableString,conditionString] = parseQuery(sys.argv[1])
    if tableString=='':
        print "Error : No Table Names were mentioned to Query the data on (From Predicate Missing)"
        return
    elif columnString=='':
        print "Error : No Attributes Were Selected for Projection (select predicate missing)"
        return
    else:
        tableList = tableString.split(',')
        columnList = columnString.split(',')
        conditionList = conditionString.split(',')
        semanticsCheck = queryCheckSemantics(columnList,tableList)
        if(semanticsCheck==0):
            return
        elif semanticsCheck==1:
            for i in range(0,len(tableList)):
                tableAppearingOrders[tableList[i]] = i
            queryExecutionModule(ClassDefinitions,columnList,tableList,conditionList,conditionString)

if __name__ == '__main__':
    main()
