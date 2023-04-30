all:
	python3 -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. CommWithMaster.proto
	python3 -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. CommWithMapper.proto
	python3 -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. CommWithReducer.proto

