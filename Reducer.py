import ast
import sys
sys.path.insert(1, 'Protos')

import os
import CommWithReducer_pb2_grpc
import CommWithReducer_pb2
from concurrent import futures
import grpc
import logging



class CommWithReducerServicer(CommWithReducer_pb2_grpc.CommWithReducerServicer):
    def connectToReducer(self, request, context):
        print("Connect to reducer called")
        startReducer(request.directory, request.typeOfRequest, request.index, request.mappers)

        return CommWithReducer_pb2.ReducerResponse(status="Reducer finished work : " + str(request.index))

def shuffleAndSort(index, mappers, typerequest):
    sortedPairs = []
    for mapper in range(1, mappers + 1):
        pathForPartition = "datafiles/intermediate/mapper" + str(mapper) + "/Inter" + str(index) + ".txt"

        with open(pathForPartition, "r") as f:
            for i, line in enumerate(f):
                if typerequest == 3 and i <=1:
                    continue
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
    for keys in sortedKeys.keys():
        with open(outputDirectory, '+a') as file:
            reduced = sum(sortedKeys[keys])
            file.write(keys + " " + str(reduced))
            file.write("\n")
 
    
def invertedIndex(outputDirectory, sortedKeys):
    for keys in sortedKeys.keys():
        with open(outputDirectory, '+a') as file:
            reduced = ""
            for id in sortedKeys[keys]:
                str_id = str(id)
                if str_id not in reduced:
                    reduced+= str_id + ", "
            file.write(keys + " " + reduced[:-2])
            file.write("\n")


def naturalJoin(outputDirectory, sortedKeys, mapper, index):
    columns = []
    pathForPartition = "datafiles/intermediate/mapper" + str(mapper) + "/Inter" + str(index) + ".txt"
    with open(pathForPartition, "r") as f:
        for i, line in enumerate(f):
            content = line.strip()
            content = content[5:-1]
            columns.append(content.split(", "))
            if i == 1:
                break

    common = ""
    for name in columns[0]:
        if name in columns[1]:
            common = name
    col1 = ''+common
    for col in columns:
        for name in col:
            if name != common:
                col1+=', '
                col1+=name
    col1 += '\n'

    with open(outputDirectory, '+a') as file:
        file.write(col1)

    for key in sortedKeys.keys():
        T1 = []
        T2 = []

        for val in sortedKeys[key]:
            if(val[0] == "T1"):
                T1.append(val[1:])
            else:
                T2.append(val[1:])

        for colT1 in T1:
            for colT2 in T2:
                row = []
                row.append(key)
                row.extend(colT1 + colT2)
                row_str = str(row)

                with open(outputDirectory, '+a') as file:
                    file.write(row_str[1:-1])
                    file.write("\n")
    

def startReducer(outputDirectory, RequestType, index, mappers):
    sortedKeys = shuffleAndSort(index, mappers, RequestType)
    if RequestType == 1:
        wordCount(outputDirectory, sortedKeys)
    elif RequestType == 2:
        invertedIndex(outputDirectory, sortedKeys)
    else:
        naturalJoin(outputDirectory, sortedKeys, mappers, index)
        
def serve(portNumber):
    print("Serve called in reducer num: ", portNumber)
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    CommWithReducer_pb2_grpc.add_CommWithReducerServicer_to_server(CommWithReducerServicer(), server)
    server.add_insecure_port('[::]:' + str(portNumber))
    server.start()
    server.wait_for_termination()

def main(args):
    logging.basicConfig
    serve(int(args[0]))

if __name__ == "__main__":
    main(sys.argv[1:])