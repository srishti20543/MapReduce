import sys
sys.path.insert(1, 'Protos')

from concurrent import futures

import CommWithMaster_pb2_grpc
import CommWithMaster_pb2
import logging
import grpc
import Mapper
import Reducer

from multiprocessing import Process


MAPPERS = 0
REDUCERS = 0
InputDir = ''
OutputDir = ''
RequestType = 0

class CommWithMasterServicer(CommWithMaster_pb2_grpc.CommWithMasterServicer):

    def MakeChoice(self, request, context):
        global RequestType
        
        if request.typeOfRequest == 1 or request.typeOfRequest == 2 or request.typeOfRequest == 3:
            RequestType = request.typeOfRequest
            forkMappers()
            # forkReducers()
            return CommWithMaster_pb2.RegisterResponse(status="SUCCESS")
        else:
            return CommWithMaster_pb2.RegisterResponse(status="FAIL")


def forkMappers():
    Mappers = []
    for i in range(MAPPERS):
            dir = InputDir + '/Input' + str(i+1) + '.txt'
            Mappers.append(Process(target=Mapper.startMapper, args=(dir, RequestType, (i+1), REDUCERS)))
            Mappers[i].start()

def forkReducers():
    Reducers = []
    for i in range(REDUCERS):
            dir = InputDir + '/Output' + str(i+1) + '.txt'
            Reducers.append(Process(target=Reducer.startReducer, args=(dir, RequestType, (i+1))))
            Reducers[i].start()

def serve():
    print(MAPPERS, REDUCERS, InputDir, OutputDir)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    CommWithMaster_pb2_grpc.add_CommWithMasterServicer_to_server(CommWithMasterServicer(), server)
    server.add_insecure_port('[::]:8000')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    arg = sys.argv[1:]
    InputDir = arg[0]
    MAPPERS = int(arg[1])
    REDUCERS = int(arg[2])
    OutputDir = arg[3]
    logging.basicConfig()
    serve()