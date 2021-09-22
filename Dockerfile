FROM openjdk:11
COPY . /user/src/myapp
WORKDIR /user/src/myapp
RUN javac helloWorld.java