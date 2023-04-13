InterDir = ""

def wordCount(InputDir, index):
    global InterDir 
    intermediate = []
    with open(InputDir, "r") as f:
        content = f.read()
    listOfWords = content.split()
    for word in listOfWords:
        intermediate.append((word, 1))

    InterDir = 'Datafiles/Intermediate/inter'+str(index)+'.txt'

    with open(InterDir, "w") as f:
        for inter in intermediate:
            f.write(str(inter)+'\n')

def partition(Reducers):
    intermediate = []
    with open(InterDir, "r") as f:
        for line in f:
            intermediate.append(line.strip())

    
    for inter in intermediate:
        string = inter[2:-5]
        partition = len(string)%Reducers
        InterDir2 = 'Datafiles/Intermediate/intermed'+str(partition+1)+'.txt'
        with open(InterDir2, "+a") as f:
            f.write(inter+'\n')


def startMapper(InputDir, RequestType, index, Reducers):
    if RequestType == 1:
        wordCount(InputDir, index)
    # elif RequestType == 2:
    #     invertedIndex(InputDir)
    # else:
    #     naturalJoin(InputDir)

    partition(Reducers)