#!/bin/bash
ssh -i "~/.ssh/pydata.pem" ubuntu@ec2-18-188-136-11.us-east-2.compute.amazonaws.com "cd temp/pykapa;
/home/ubuntu/.local/bin/docker-compose run pykapa python -m pykapa $@"
