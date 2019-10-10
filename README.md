# Pykapa
    This package is designed to have data collection and monitoring tools freely accessible to researchers.
    Researchers use it to monitor fieldworkers data collection on slack channels and respond timely to correct their mistakes.
    The package reads an XLS form containing conditions on the collected data and posts messages to relevant slack
    channels when the conditions are satisfied.
    if incentives are associated with the research, then airtime, SMS or data bundle incentives are awarded to respondents.
    However, this version only works for data stored and collected via surveyCTO.

# ikapa data Deployment ie: (technical people)
    for changes that you want to go live
    
##1) Update the version file
    (open version file and increment from 0.0.1 to 0.0.2)
    
##2) Add a git commit (preferably with the version as part of the commit name)
    git add .
    git commit -m '0.0.2 updated things for slack api'
    
##3) Merge to Master and Push
    git checkout master
    git merge branchname
    git push -u origin master
    
##4.1) Ssh into your server and update the 'live version' in the docker-compose file. (this version controls rollbacks easiliy)
    ssh serveruser@servername
    (open docker-compose file and change the 0.0.1 to 0.0.2 next to the image name pykapa:0.0.1)
#####OR 
#####4.2) Local deployment
    pull image from docker with the correct version docker run pykapa:v0.0.2

# usage for nontechnical people
## for running the service
    1) Users must be supplied with the run_pykapa.sh script, and the .pem key file
    2) Users can the type: "./run_pykapa.sh -u=Username -p=password -s=channel -url=www.some.url.like.usual"
