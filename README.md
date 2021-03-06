# logspout-loggly
Logspout container for Docker and Loggly.

[This repo follows this suggested pattern from logspout](https://github.com/gliderlabs/logspout/tree/master/custom)

You can build the image yourself

```docker build -t your-user-name/logspout-loggly ./```

and, optionally, push it to your own hub.docker.com repo

```docker push your-user-name/logspout-loggly```

or you can pull a prebuilt image

```docker pull iamatypeofwalrus/logspout-loggly```

## How to run

```sh
docker run -d \
  -e 'LOGGLY_TOKEN=<token>' \
  -e 'LOGGLY_TAGS=<comma-delimited-list>' \
  -e 'FILTER_NAME=<wildcard-container-name>' \
  --volume /var/run/docker.sock:/tmp/docker.sock \
  iamatypeofwalrus/logspout-loggly
```

## How it works
Instead of linking containers together or bothering with [syslog or remote syslog](https://www.loggly.com/blog/centralize-logs-docker-containers) this container follows the [12 Factor app logging philosophy](http://12factor.net/logs). If your docker container(s) log(s) to STDOUT this image will pick up that stream from the docker daemon send those events to Loggly.
