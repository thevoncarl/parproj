#Installation isntructions

For each celery worker:

```bash
sudo apt-get install python-pip
sudo pip install celery
```
Run a celery worker:
```bash
celery -A Celworker worker -c 4
```

For the master node:

```bash
sudo apt-get install python-pip
sudo pip install celery

sudo apt-get install rabbitmq-server
```

Example setup user credentials for rabbitmq

```bash
rabbitmqctl add_user test test
rabbitmqctl set_user_tags test administrator
rabbitmqctl set_permissions -p / test ".*" ".*" ".*"

```


