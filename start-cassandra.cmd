rem docker stack deploy -c cassandra.yml cassandra
rem timeout 1
rem docker ps

docker run --name cassandra -p 9042:9042 -d cassandra:latest
