# Pull base image
FROM python:3.7
# Set environment varibles
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
WORKDIR /code/
# Copy requirements.txt and Pipfile/Pipfile.lock to container
COPY requirements.txt  /code/
RUN pip install -r requirements.txt
# Install dependencies
RUN pip install pipenv
RUN pip install -r requirements.txt
COPY . /code/
EXPOSE 8000
CMD ["python", "etl_web_apis.py"]