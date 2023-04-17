import os

InterDir = ""
Reducers = 0

def partition(intermediate, index):
    mapper_dir = 'datafiles/intermediate/mapper'+str(index)
    if not os.path.exists(mapper_dir):
        os.makedirs(mapper_dir)

    for inter in intermediate:
        string = str(inter)
        partition = len(string[2:-5])%Reducers
        InterDir = mapper_dir+'/Inter'+str(partition+1)+'.txt'

        with open(InterDir, "+a") as f:
            f.write(str(inter))
            f.write("\n")


def wordCount(InputDir, index):
    global InterDir 
    intermediate = []

    with open(InputDir, "r") as f:
        content = f.read()

    listOfWords = content.split()
    for word in listOfWords:
        intermediate.append((word, 1))

    partition(intermediate, index)


def invertedIndex(InputDir, index):
    global InterDir 
    intermediate = []

    with open(InputDir, "r") as f:
        content = f.read()

    listOfWords = content.split()
    for word in listOfWords:
        intermediate.append((word, index))

    partition(intermediate, index)


def naturalJoin(InputDir, index):
    pass


def startMapper(InputDir, RequestType, index, Reducer):
    global Reducers
    Reducers = Reducer

    if RequestType == 1:
        wordCount(InputDir, index)
    elif RequestType == 2:
        invertedIndex(InputDir, index)
    else:
        naturalJoin(InputDir, index)