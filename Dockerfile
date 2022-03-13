FROM python:3.8

RUN python3 -m pip install --upgrade pip
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y libgeos-dev

COPY . .
RUN python3 -m pip install -r requirements.txt
RUN python3 -m pip install git+https://github.com/SpielerNogard/Pokefinder-Package.git

CMD ["python3","-u","bot.py"]
