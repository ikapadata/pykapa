#!/bin/bash
ssh -i "~/.ssh/pydata.pem" ubuntu@ec2-18-188-136-11.us-east-2.compute.amazonaws.com "/home/ubuntu/.local/bin/docker-compose pull;/home/ubuntu/.local/bin/docker-compose run pykapa python -m pykapa $@"
