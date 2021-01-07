FROM registry.redhat.io/ubi8/go-toolset

COPY . .
RUN mkdir bin && make

USER nobody
# CMD ["go", "./bin/main"]
CMD ["/bin/sh"]
