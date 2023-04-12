import sys
sys.path.insert(1, 'Protos')

from concurrent import futures

import CommWithMaster_pb2_grpc
import CommWithMaster_pb2
import logging
import grpc

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
            return CommWithMaster_pb2.RegisterResponse(status="SUCCESS")
        else:
            return CommWithMaster_pb2.RegisterResponse(status="FAIL")

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
