FROM python:3.7.4-stretch
RUN apt-get update
RUN apt-get install libgirepository1.0-dev gcc libcairo2-dev pkg-config python3-dev gir1.2-gtk-3.0 -y
WORKDIR /usr/local/ikapa/
COPY ./requirements.txt .
RUN pip install -r requirements.txt
ARG google_sheet_url
ARG username
ARG password
ARG server
ARG gitcommithash
COPY ./pykapa ./pykapa
#CMD python -m pykapa

