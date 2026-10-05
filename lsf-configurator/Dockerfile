# Build stage: use Debian-based Go image for full CGO support
FROM golang:1.24.0-bookworm AS build

RUN apt-get update && apt-get install -y build-essential git && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . /app
RUN CGO_ENABLED=1 GOOS=linux go build -a -o lsf-configurator

# Build stage for React frontend
FROM node:22-alpine AS frontend-build
WORKDIR /frontend
COPY ./frontend /frontend
RUN npm install && npm run build

# Final image: minimal Debian-based Go image with CGO support
FROM golang:1.24.0-bookworm

RUN apt-get update && \
    # Install Python 3.11 and necessary development headers and pip
    apt-get install -y build-essential python3.11 python3.11-dev python3-pip git && \
    # Ensure python3 points to 3.11.
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    update-alternatives --set python3 /usr/bin/python3.11 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /
COPY --from=build /app/lsf-configurator /lsf-configurator
COPY --from=frontend-build /frontend/dist /public
COPY --from=build /app/pkg/layout/ /layout
COPY --from=build /app/templates /templates

# Install python packages globally using the override flag to bypass PEP 668.
RUN pip3 install --break-system-packages networkx slambuc

EXPOSE 8080
CMD ["/lsf-configurator"]
