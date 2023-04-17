from __future__ import print_function

import sys
sys.path.insert(1, 'Protos')

import CommWithMaster_pb2_grpc
import CommWithMaster_pb2
import grpc
import logging

def connectToMaster(inputs):
    with grpc.insecure_channel('localhost:8000') as channel:
        stub = CommWithMaster_pb2_grpc.CommWithMasterStub(channel)
        request = CommWithMaster_pb2.RegisterRequest(typeOfRequest=inputs[0], in_dir=inputs[1], out_dir=inputs[2], mappers=inputs[3], reducers=inputs[4])
        status = stub.MakeChoice(request)
        print(status)

if __name__ == '__main__':
    logging.basicConfig()
    inputs = []
    print("Choose Option: \n1. Word Count \n2. Inverted Index [Record-Level Inverted index]\n3. Natural Join")
    inputs.append(int(input()))
    print("Enter Input Directory")
    inputs.append(input())
    print("Enter Output Directory")
    inputs.append(input())
    print("Enter number of Mappers")
    inputs.append(int(input()))
    print("Enter number of Reducers")
    inputs.append(int(input()))
    connectToMaster(inputs)
