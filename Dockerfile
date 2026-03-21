# --- Stage 1: Build the provider binary ---
FROM golang:1.24 AS builder

WORKDIR /workspace

# Copy the Go module manifests and download dependencies
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

# Copy the Go source code
# (Adjust these paths if your project structure is different)
COPY cmd/ cmd/
COPY internal/ internal/
COPY apis/ apis/

# Build the binary statically so it runs without C dependencies
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o provider cmd/provider/main.go


# --- Stage 2: Create the minimal runtime image ---
# Using distroless for a minimal, secure execution environment
FROM gcr.io/distroless/static:nonroot

WORKDIR /

# Copy the binary from the builder stage
COPY --from=builder /workspace/provider .

# Run as a non-root user for security
USER 65532:65532

# Set the entrypoint to the compiled provider binary
ENTRYPOINT ["/provider"]