cat version | xargs -I {} docker build -t pykapa:v{} .

