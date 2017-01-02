# Govel

Govel is an **experiment** to build a [rabbitmq shovel](https://www.rabbitmq.com/shovel.html)-like in Go


# Usage

```
 -from-queue string
    	Queue to read messages from
  -from-url string
    	Source rabbit connection url (amqp://guest:guest@rabbitmq-server-A:5672/)
  -to-exchange string
    	Exchange to write messages to
  -to-url string
    	Target rabbit connection url (amqp://guest:guest@rabbitmq-server-B:5672/)
```

## Sample command line

```
./govel --from-url "amqp://guest:guest@10.0.3.214:5672/" \
--from-queue Q_test \
--to-url "amqp://guest:guest@10.0.3.254:5672/" \
--to-exchange E_test
```

# Building govel


```bash
. ./goenv.sh
go get github.com/streadway/amqp
go build govel.go
```

