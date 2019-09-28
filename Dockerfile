FROM  python:3.7

COPY requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . /app

CMD ["python3", "-m", "src"]

