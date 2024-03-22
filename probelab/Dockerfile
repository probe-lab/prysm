# Create lightweight image. Use Debian as base because we have enabled CGO
# above and hence compile against glibc. This means we can't use alpine.
FROM debian:latest

# Create user prysm
RUN adduser --system --no-create-home --disabled-login --group prysm
WORKDIR /home/prysm
USER prysm

COPY ./beacon-chain beacon-chain

CMD ["beacon-chain"]