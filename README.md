## Kratos Project
```
doc: https://go-kratos.dev/docs/
code: https://github.com/go-kratos/kratos
```

## Build && Run
```
go build -o ./bin/ ./...
./bin/server -conf ./configs
```

## Docker
```bash
# build
docker build -t <your-docker-image-name> .

# run
docker run --rm -p 8666:8666 -v </path/to/your/configs>:/data/conf <your-docker-image-name>
```
