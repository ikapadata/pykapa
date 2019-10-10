# pykapa
This package is designed to have data collection and monitoring tools freely accessible to researchers.
 Researchers use it to monitor fieldworkers data collection on slack channels and respond timely to correct their mistakes.
 The package reads an XLS form containing conditions on the collected data and posts messages to relevant slack
 channels when the conditions are satisfied.
 if incentives are associated with the research, then airtime, SMS or data bundle incentives are awarded to respondents.
  However, this version only works for data stored and collected via surveyCTO.


# ikapa data Deployment
for changes that you want to go live
1) Update the version file
2) Add a git commit (preferably with the version as part of the commit name)
3) Merge to Master and Push
4) Ssh into the server and update the 'live version' in the docker-compose file. (this version controls rollbacks easiliy)

# usage
for running the service
1) Users must be supplied with the run_pykapa.sh script, and the .pem key file
2) Users can the type: "./run_pykapa.sh -u=Username -p=password -s=channel -url=www.some.url.like.usual"
