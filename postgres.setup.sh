
sudo -u postgres createuser --superuser --pwprompt vumi

createdb -W -U vumi -h localhost -E UNICODE staging

createdb -W -U vumi -h localhost -E UNICODE production

createdb -W -U vumi -h localhost -E UNICODE unstable

pip -E ve install psycopg2


