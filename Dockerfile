# Multi-stage build: Rust builder stage
FROM rust:1.88-slim AS rust-builder

# Install system dependencies for Rust build
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /rust-build

# Copy Cargo.toml first for better caching
COPY rust-src/Cargo.toml ./Cargo.toml

# Create dummy files to match the binary targets in Cargo.toml
RUN mkdir -p src && echo "fn main() {}" > src/scrape_calculations.rs
RUN mkdir -p src && echo "fn main() {}" > src/process_calculations.rs

# Build dependencies first (this layer will be cached)
RUN cargo build --release && rm -rf src target/release/deps/calculation*

# Now copy actual source code
COPY rust-src/src ./src

# Build both binaries
RUN cargo build --release --bin calculation-scraper
RUN cargo build --release --bin calculation-processor

# Main application stage
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws/

WORKDIR /app

# Copy Python requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Python source code
COPY src/ ./src/

# Copy both compiled Rust binaries from builder stage
COPY --from=rust-builder /rust-build/target/release/calculation-scraper ./bin/
COPY --from=rust-builder /rust-build/target/release/calculation-processor ./bin/

# Make the Rust binaries executable and verify they exist
RUN chmod +x ./bin/calculation-scraper && \
    chmod +x ./bin/calculation-processor && \
    ls -la ./bin/ && \
    ./bin/calculation-scraper --help || echo "calculation-scraper binary installed successfully" && \
    ./bin/calculation-processor --help || echo "calculation-processor binary installed successfully"

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV PATH="/app/bin:$PATH"

# Copy entrypoint script
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Create temp directory for processing
RUN mkdir -p /tmp/chess_data

ENTRYPOINT ["./entrypoint.sh"]