FROM python:3.10.6

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

RUN pip3 install google-auth-oauthlib

RUN pip3 install google-api-python-client

ADD update_model.py .

ADD Google.py .

ADD client_secret_910443753452-46uls2n5rauh4g24erkpgulehhsaf6g1.apps.googleusercontent.com.json .