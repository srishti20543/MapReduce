import os
import ast

def shuffleAndSort(index, mappers):
    sortedPairs = []

    for mapper in range(1, mappers + 1):
        pathForPartition = "datafiles/intermediate/mapper" + str(mapper) + "/Inter" + str(index) + ".txt"

        with open(pathForPartition, "r") as f:
            for line in f:
                content = line.strip()
                tupleObj = ast.literal_eval(content)
                sortedPairs.append(tupleObj)

    sortedPairs = sorted(sortedPairs, key=lambda x: x[0])
    groupedValues = {}
    for keyValuePair in sortedPairs:
        if keyValuePair[0].lower() not in groupedValues:
            groupedValues[keyValuePair[0].lower()] = []
        groupedValues[keyValuePair[0].lower()].append(keyValuePair[1])

    return groupedValues


def wordCount(outputDirectory, sortedKeys):
    # Open the file in write mode
    for keys in sortedKeys.keys():
        with open(outputDirectory, '+a') as file:
            # Write data to the file
            reduced = sum(sortedKeys[keys])
            file.write(keys + " " + str(reduced))
            file.write("\n")
 
    
def invertedIndex(outputDirectory, sortedKeys):
    for keys in sortedKeys.keys():
        with open(outputDirectory, '+a') as file:
            # Write data to the file
            reduced = ""
            for id in sortedKeys[keys]:
                str_id = str(id)
                if str_id not in reduced:
                    reduced+= str_id + ", "
            file.write(keys + " " + reduced[:-2])
            file.write("\n")

def naturalJoin(InputDir, index):
    pass


def startReducer(outputDirectory, RequestType, index, mappers):

    sortedKeys = shuffleAndSort(index, mappers)
    if RequestType == 1:
        wordCount(outputDirectory, sortedKeys)
    elif RequestType == 2:
        invertedIndex(outputDirectory, sortedKeys)
    else:
        naturalJoin(outputDirectory, sortedKeys)
