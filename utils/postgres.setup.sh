
sudo -u postgres createuser --superuser --pwprompt vumi

createdb -W -U vumi -h localhost -E UNICODE vumi

createdb -W -U vumi -h localhost -E UNICODE development

createdb -W -U vumi -h localhost -E UNICODE staging

createdb -W -U vumi -h localhost -E UNICODE production

createdb -W -U vumi -h localhost -E UNICODE develop


