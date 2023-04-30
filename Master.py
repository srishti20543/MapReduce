import sys
sys.path.insert(1, 'Protos')

from concurrent import futures
from threading import Thread
import CommWithMaster_pb2_grpc
import CommWithMaster_pb2
import CommWithMapper_pb2
import CommWithMapper_pb2_grpc
import CommWithReducer_pb2
import CommWithReducer_pb2_grpc
import logging
import grpc
import Mapper
import Reducer
import os
from time import sleep

MAPPERS = 0
REDUCERS = 0
MAPPERS_Actual = 0
InputDir = ''
OutputDir = ''
RequestType = 0
portsForMappers = []
portsForReducers = []

class CommWithMasterServicer(CommWithMaster_pb2_grpc.CommWithMasterServicer):
    
    def MakeChoice(self, request, context):
        global RequestType, MAPPERS, REDUCERS, InputDir, OutputDir, portsForMappers, portsForReducers
        mapper_start_num = 7000
        reducer_start_num = 6000
        
        if request.typeOfRequest == 1 or request.typeOfRequest == 2 or request.typeOfRequest == 3:
            RequestType = request.typeOfRequest
            MAPPERS = request.mappers
            REDUCERS = request.reducers
            InputDir = request.in_dir
            OutputDir = request.out_dir
            for i in range(MAPPERS):
                portsForMappers.append(mapper_start_num + i)
            for i in range(REDUCERS):
                portsForReducers.append(reducer_start_num + i)
            print("number of mappers: ", MAPPERS)
            print("number of reducers: ", REDUCERS)

            connectToMappers()
            connectToReducers()
            return CommWithMaster_pb2.RegisterResponse(status="SUCCESS")
        else:
            return CommWithMaster_pb2.RegisterResponse(status="FAIL")

def startConnectionWithMapper(mapperPorts, directories, RequestType, index, REDUCERS, ids):
    # global portsForMappers
    port_number = str(mapperPorts[index - 1])
    print("Mapper called with: ", port_number)

    request = CommWithMapper_pb2.MappingRequest()
    request.directories.extend(directories)
    request.ids.extend(ids)
    request.typeOfRequest = RequestType
    request.index = index
    request.reducers = REDUCERS

    with grpc.insecure_channel('localhost:' + port_number) as channel:
        stub = CommWithMapper_pb2_grpc.CommWithMapperStub(channel)
        print("Sending request to stub: ", port_number)
        status = stub.connectToMapper(request)
        print(status)

def connectToMappers():
    global MAPPERS_Actual, portsForMappers

    if not os.path.exists('datafiles/intermediate'):
        os.makedirs('datafiles/intermediate')

    if RequestType == 1:
        files = os.listdir(InputDir + '/word_count')
        num_files = len(files)
    elif RequestType == 2:
        files = os.listdir(InputDir + '/inverted_index')
        num_files = len(files)
    else:
        files = os.listdir(InputDir + '/natural_join')
        num_files = len(files)//2

    Mapper_Threads = []
    if num_files <= MAPPERS:
        MAPPERS_Actual = num_files
        for i in range(num_files):
            dir = []
            ids = []
            if RequestType == 1:
                dir.append(InputDir + '/word_count/Input' + str(i+1) + '.txt')
            elif RequestType == 2:
                dir.append(InputDir + '/inverted_index/Input' + str(i+1) + '.txt')
                ids.append(str(i+1))
            elif RequestType == 3:
                dir.append(InputDir + '/natural_join/Input' + str(i+1))
            Mapper_Threads.append(Thread(target=startConnectionWithMapper, args=(portsForMappers, dir, RequestType, (i+1), REDUCERS, ids)))
            # Mappers[-1].start()
    else:
        MAPPERS_Actual = MAPPERS
        for i in range(MAPPERS):
            dir = []
            ids = []
            j = i
            while j+1 <= num_files:
                if RequestType == 1:
                    dir.append(InputDir + '/word_count/Input' + str(j+1) + '.txt')
                elif RequestType == 2:
                    dir.append(InputDir + '/inverted_index/Input' + str(j+1) + '.txt')
                    ids.append(str(j+1))
                else:
                    dir.append(InputDir + '/natural_join/Input' + str(j+1))
                j+=MAPPERS
            Mapper_Threads.append(Thread(target=startConnectionWithMapper, args=(portsForMappers, dir, RequestType, (i+1), REDUCERS, ids)))
    
    for thread in Mapper_Threads:
        thread.start()

    for thread in Mapper_Threads:
        thread.join()
    print("Mapping Threads Finished")

def ReducerCaller(index):
    global MAPPERS_Actual

    dir = OutputDir + '/Output' + str(index+1) + '.txt'
    port_number = str(portsForReducers[index])
    request = CommWithReducer_pb2.ReducerRequest()
    request.directory = dir
    request.typeOfRequest = RequestType
    request.index = (index+1)
    request.mappers = MAPPERS_Actual
    with grpc.insecure_channel('localhost:' + port_number) as channel:
        stub = CommWithReducer_pb2_grpc.CommWithReducerStub(channel)
        print("Sending request to reducer stub: ", port_number)
        status = stub.connectToReducer(request)
        print(status)


def connectToReducers():
    global portsForReducers,MAPPERS_Actual

    if not os.path.exists('datafiles/output'):
        os.makedirs('datafiles/output')

    Reducer_Threads= []
    for i in range(REDUCERS):
        Reducer_Threads.append(Thread(target=ReducerCaller, args=(i,)))

    for thread in Reducer_Threads:
        thread.start()
    
    for thread in Reducer_Threads:
        thread.join()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    CommWithMaster_pb2_grpc.add_CommWithMasterServicer_to_server(CommWithMasterServicer(), server)
    server.add_insecure_port('[::]:8000')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
    