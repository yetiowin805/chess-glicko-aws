# Multi-stage build: Rust builder stage
FROM rust:1.77-slim as rust-builder

# Install system dependencies for Rust build
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /rust-build

# Copy Rust source code
COPY rust-src/ ./

# Build the Rust application
RUN cargo build --release

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

# Copy the compiled Rust binary from builder stage
COPY --from=rust-builder /rust-build/target/release/calculation-scraper ./bin/

# Make the Rust binary executable
RUN chmod +x ./bin/calculation-scraper

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