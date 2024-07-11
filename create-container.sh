docker run --name postgres_r1 -e POSTGRES_PASSWORD=ihavethepower -d -p 5433:5432 postgres

docker exec -it <container> bash

cd /var/lib/postgresql/data