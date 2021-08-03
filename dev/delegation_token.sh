docker-compose exec broker kafka-delegation-tokens --bootstrap-server broker:29093 --create --max-life-time-period -1 --command-config /etc/kafka/client.config --renewer-principal User:admin
docker-compose exec broker kafka-delegation-tokens --bootstrap-server broker:29093 --describe --command-config /etc/kafka/client.config --owner-principal User:admin
