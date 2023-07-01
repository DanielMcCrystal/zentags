import itertools

path = "path/to/file"


for sublist in itertools.accumulate(path.split("/"), lambda x, y: f"{x}/{y}"):
    print(sublist)
