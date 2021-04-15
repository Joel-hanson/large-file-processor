# large-file-processor-postman
Aim is to build a system which is able to handle long running processes in a distributed fashion.

Requirements:
- Docker
- python
- python packages in requirements.txt


docker pull postgres
docker run -d --name postman-postgres  -v $PWD:/data -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:latest
docker ps
docker exec -it postman-postgres psql -U postgres