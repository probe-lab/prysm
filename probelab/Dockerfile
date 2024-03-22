FROM debian:latest

RUN adduser --system --no-create-home --disabled-login --group prysm
WORKDIR /home/prysm
USER prysm

COPY ./beacon-chain beacon-chain

CMD ["beacon-chain"]
