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

def partitionNaturalJoin(pairs1, pairs2, index):
    hashKeys = {}
    for key in pairs1.keys():
        if key not in hashKeys.keys():
            hashKeys[key] = hash(key)

    for key in pairs2.keys():
        if key not in hashKeys.keys():
            hashKeys[key] = hash(key)

    mapper_dir = 'datafiles/intermediate/mapper'+str(index)
    if not os.path.exists(mapper_dir):
        os.makedirs(mapper_dir)

    for inter in pairs1.keys():
        for i in range(len(pairs1[inter])):
            tupleToWrite = (inter, pairs1[inter][i])
            partition = hashKeys[inter] % Reducers
            InterDir = mapper_dir+'/Inter'+str(partition+1)+'.txt'

            with open(InterDir, "+a") as f:
                f.write(str(tupleToWrite))
                f.write("\n")
        
    for inter in pairs2.keys():
        for i in range(len(pairs2[inter])):
            tupleToWrite = (inter, pairs2[inter][i])
            partition = hashKeys[inter] % Reducers
            InterDir = mapper_dir+'/Inter'+str(partition+1)+'.txt'

            with open(InterDir, "+a") as f:
                f.write(str(tupleToWrite))
                f.write("\n")


def wordCount(InputDir, index):
    global InterDir 
    intermediate = []
    for dir in InputDir:
        with open(dir, "r") as f:
            content = f.read()

        listOfWords = content.split()
        for word in listOfWords:
            intermediate.append((word, 1))

        partition(intermediate, index)


def invertedIndex(InputDir, index, ids):
    global InterDir 
    intermediate = []
    for i, dir in enumerate(InputDir):
        with open(dir, "r") as f:
            content = f.read()

        listOfWords = content.split()
        for word in listOfWords:
            intermediate.append((word, ids[i]))

        partition(intermediate, index)


def naturalJoin(InputDir, index):
    InputDir1 = InputDir[0] + '_table1.txt'
    InputDir2 = InputDir[0] + '_table2.txt'
    values_tab1 = []
    values_tab2 = []
    columns = []

    with open(InputDir1, "r") as f:
        for i, line in enumerate(f):
            content = line.strip()
            if i == 0:
                columns.append(content.split(", "))
            else:
                values_tab1.append(content.split(", "))

    with open(InputDir2, "r") as f:
        for i, line in enumerate(f):
            content = line.strip()
            if i == 0:
                columns.append(content.split(", "))
            else:
                values_tab2.append(content.split(", "))

    common_element = list(set(columns[0]).intersection(set(columns[1])))

    ind_tb1 = columns[0].index(common_element[0])
    ind_tb2 = columns[1].index(common_element[0])

    pairs1 = {}
    pairs2 = {} 

    for i in range(len(values_tab1)):
        if values_tab1[i][ind_tb1] not in pairs1.keys():
            intermed = []
        else:
            intermed = pairs1[values_tab1[i][ind_tb1]]

        tupleToAppend = ("T1", )
        for j in range(len(values_tab1[i])):
            if j != ind_tb1:
                tupleToAppend = tupleToAppend + (values_tab1[i][j],)
        
        intermed.append(tupleToAppend)
        pairs1[values_tab1[i][ind_tb1]] = intermed

    for i in range(len(values_tab2)):
        if values_tab2[i][ind_tb2] not in pairs2.keys():
            intermed = []
        else:
            intermed = pairs2[values_tab2[i][ind_tb2]]

        tupleToAppend = ("T2", )
        for j in range(len(values_tab2[i])):
            if j != ind_tb2:
                tupleToAppend = tupleToAppend + (values_tab2[i][j], )
        
        intermed.append(tupleToAppend)
        pairs2[values_tab2[i][ind_tb2]] = intermed

    # print(pairs1, pairs2)
    partitionNaturalJoin(pairs1, pairs2, index)


def startMapper(InputDir, RequestType, index, Reducer, ids):
    global Reducers
    Reducers = Reducer

    if RequestType == 1:
        wordCount(InputDir, index)
    elif RequestType == 2:
        invertedIndex(InputDir, index, ids)
    else:
        naturalJoin(InputDir, index)