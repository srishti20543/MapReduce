from __future__ import print_function

import sys
sys.path.insert(1, 'Protos')

import CommWithMaster_pb2_grpc
import CommWithMaster_pb2
import grpc
import logging

def connectToMaster(choice):
    with grpc.insecure_channel('localhost:8000') as channel:
        stub = CommWithMaster_pb2_grpc.CommWithMasterStub(channel)
        request = CommWithMaster_pb2.RegisterRequest(typeOfRequest = choice)
        status = stub.MakeChoice(request)
        print(status)

if __name__ == '__main__':
    logging.basicConfig()
    print("Choose Option: \n1. Word Count \n2. Inverted Index [Record-Level Inverted index]\n3. Natural Join")
    choice = int(input())
    connectToMaster(choice)
