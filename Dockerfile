# Use an official Python runtime as a parent image
FROM golang:latest

# Set the working directory to /app
WORKDIR /go/src/backend_chat

# Copy the current directory contents into the container at /app
ADD . /go/src/backend_chat


ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.6.0/wait /wait
RUN chmod +x /wait
RUN chmod +x ./start.sh

CMD ./start.sh

# Make port 8081 available to the world outside this container
EXPOSE 8081
