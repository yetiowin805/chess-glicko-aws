#!/bin/bash
set -e

# Configuration
PROJECT_NAME="chess-glicko"
AWS_REGION="${AWS_REGION:-us-east-2}"
NOTIFICATION_EMAIL="${NOTIFICATION_EMAIL:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        error "AWS CLI is not installed"
        exit 1
    fi
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed"
        exit 1
    fi
    
    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        error "Terraform is not installed"
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        error "AWS credentials not configured"
        exit 1
    fi
    
    log "Prerequisites check passed"
}

# Deploy infrastructure
deploy_infrastructure() {
    log "Deploying AWS infrastructure..."
    
    cd terraform
    
    # Initialize Terraform
    terraform init
    
    # Create terraform.tfvars
    cat > terraform.tfvars <<EOF
aws_region = "$AWS_REGION"
project_name = "$PROJECT_NAME"
notification_email = "$NOTIFICATION_EMAIL"
EOF
    
    # Plan and apply
    terraform plan
    terraform apply -auto-approve
    
    cd ..
    log "Infrastructure deployment completed"
}

# Build and push Docker image
build_and_push_image() {
    log "Building and pushing Docker image..."
    
    # Get ECR repository URL
    ECR_REPO=$(cd terraform && terraform output -raw ecr_repository_url)
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    
    log "ECR Repository: $ECR_REPO"
    
    # Login to ECR
    aws ecr get-login-password --region $AWS_REGION | \
        docker login --username AWS --password-stdin $ECR_REPO
    
    # Build image
    log "Building Docker image..."
    docker build -t $PROJECT_NAME:latest .
    
    # Tag for ECR
    docker tag $PROJECT_NAME:latest $ECR_REPO:latest
    docker tag $PROJECT_NAME:latest $ECR_REPO:v$(date +%Y%m%d-%H%M%S)
    
    # Push to ECR
    log "Pushing to ECR..."
    docker push $ECR_REPO:latest
    docker push $ECR_REPO:v$(date +%Y%m%d-%H%M%S)
    
    log "Docker image pushed successfully"
}

# Test the pipeline
test_pipeline() {
    log "Testing the pipeline..."
    
    # Get infrastructure details
    cd terraform
    CLUSTER_NAME=$(terraform output -raw ecs_cluster_name)
    TASK_DEF=$(terraform output -raw task_definition_arn)
    S3_BUCKET=$(terraform output -raw s3_bucket_name)
    cd ..
    
    # Run a test task for the previous month
    TEST_MONTH=$(date -d "$(date +'%Y-%m-01') -1 month" +'%Y-%m')
    log "Running test for month: $TEST_MONTH"
    
    TASK_ARN=$(aws ecs run-task \
        --cluster $CLUSTER_NAME \
        --task-definition $TASK_DEF \
        --launch-type FARGATE \
        --network-configuration "awsvpcConfiguration={subnets=[$(aws ec2 describe-subnets --filters "Name=default-for-az,Values=true" --query 'Subnets[0].SubnetId' --output text)],assignPublicIp=ENABLED}" \
        --overrides "{\"containerOverrides\":[{\"name\":\"chess-glicko\",\"environment\":[{\"name\":\"PROCESS_MONTH\",\"value\":\"$TEST_MONTH\"},{\"name\":\"S3_BUCKET\",\"value\":\"$S3_BUCKET\"}]}]}" \
        --query 'tasks[0].taskArn' \
        --output text)
    
    log "Test task started: $TASK_ARN"
    log "Monitor progress with: aws ecs describe-tasks --cluster $CLUSTER_NAME --tasks $TASK_ARN"
    log "View logs with: aws logs tail /ecs/$PROJECT_NAME --follow"
    
    # Wait a bit and check status
    sleep 30
    TASK_STATUS=$(aws ecs describe-tasks \
        --cluster $CLUSTER_NAME \
        --tasks $TASK_ARN \
        --query 'tasks[0].lastStatus' \
        --output text)
    
    log "Task status: $TASK_STATUS"
    
    if [ "$TASK_STATUS" = "RUNNING" ]; then
        log "Test task is running successfully!"
        log "Check S3 bucket $S3_BUCKET for results"
        log "Expected files:"
        log "  - s3://$S3_BUCKET/persistent/player_info/raw/$TEST_MONTH.txt"
        log "  - s3://$S3_BUCKET/results/$TEST_MONTH/completion.txt"
    else
        warn "Task may have issues. Check logs for details."
    fi
}

# Setup GitHub Actions (optional)
setup_github_actions() {
    if [ "$1" = "--skip-github" ]; then
        return
    fi
    
    log "Setting up GitHub Actions workflow..."
    
    mkdir -p .github/workflows
    
    # Get values from Terraform
    cd terraform
    ECR_REPO=$(terraform output -raw ecr_repository_url)
    CLUSTER_NAME=$(terraform output -raw ecs_cluster_name)
    TASK_DEF_FAMILY=$(echo $(terraform output -raw task_definition_arn) | cut -d'/' -f2 | cut -d':' -f1)
    cd ..
    
    cat > .github/workflows/deploy.yml <<EOF
name: Deploy Chess Rating Pipeline

on:
  push:
    branches: [ main, master ]
  workflow_dispatch:

env:
  AWS_REGION: $AWS_REGION
  ECR_REPOSITORY: $ECR_REPO
  ECS_CLUSTER: $CLUSTER_NAME
  ECS_TASK_DEFINITION: $TASK_DEF_FAMILY

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout
      uses: actions/checkout@v3

    - name: Configure AWS credentials
      uses: aws-actions/configure-aws-credentials@v2
      with:
        aws-access-key-id: \${{ secrets.AWS_ACCESS_KEY_ID }}
        aws-secret-access-key: \${{ secrets.AWS_SECRET_ACCESS_KEY }}
        aws-region: \${{ env.AWS_REGION }}

    - name: Login to Amazon ECR
      id: login-ecr
      uses: aws-actions/amazon-ecr-login@v1

    - name: Build, tag, and push image to Amazon ECR
      env:
        ECR_REGISTRY: \${{ steps.login-ecr.outputs.registry }}
        IMAGE_TAG: \${{ github.sha }}
      run: |
        docker build -t \$ECR_REGISTRY/\$ECR_REPOSITORY:latest .
        docker build -t \$ECR_REGISTRY/\$ECR_REPOSITORY:\$IMAGE_TAG .
        docker push \$ECR_REGISTRY/\$ECR_REPOSITORY:latest
        docker push \$ECR_REGISTRY/\$ECR_REPOSITORY:\$IMAGE_TAG

    - name: Update ECS task definition
      run: |
        # Get current task definition
        aws ecs describe-task-definition \
          --task-definition \$ECS_TASK_DEFINITION \
          --query taskDefinition > task-def.json
        
        # Update image in task definition
        jq --arg IMAGE "\$ECR_REGISTRY/\$ECR_REPOSITORY:latest" \
          '.containerDefinitions[0].image = \$IMAGE | del(.taskDefinitionArn) | del(.revision) | del(.status) | del(.requiresAttributes) | del(.placementConstraints) | del(.compatibilities) | del(.registeredAt) | del(.registeredBy)' \
          task-def.json > new-task-def.json
        
        # Register new task definition
        aws ecs register-task-definition \
          --cli-input-json file://new-task-def.json

    - name: Trigger manual test run
      if: github.event_name == 'workflow_dispatch'
      run: |
        TEST_MONTH=\$(date -d "\$(date +'%Y-%m-01') -1 month" +'%Y-%m')
        aws ecs run-task \
          --cluster \$ECS_CLUSTER \
          --task-definition \$ECS_TASK_DEFINITION \
          --launch-type FARGATE \
          --network-configuration "awsvpcConfiguration={subnets=[\$(aws ec2 describe-subnets --filters "Name=default-for-az,Values=true" --query 'Subnets[0].SubnetId' --output text)],assignPublicIp=ENABLED}" \
          --overrides "{\"containerOverrides\":[{\"name\":\"chess-glicko\",\"environment\":[{\"name\":\"PROCESS_MONTH\",\"value\":\"\$TEST_MONTH\"}]}]}"
EOF

    log "GitHub Actions workflow created"
    log "Add these secrets to your GitHub repository:"
    echo "  - AWS_ACCESS_KEY_ID"
    echo "  - AWS_SECRET_ACCESS_KEY"
}

# Main deployment function
main() {
    log "Starting Chess Glicko Pipeline deployment"
    
    # Parse arguments
    SKIP_GITHUB=false
    for arg in "$@"; do
        case $arg in
            --skip-github)
                SKIP_GITHUB=true
                shift
                ;;
            --email=*)
                NOTIFICATION_EMAIL="${arg#*=}"
                shift
                ;;
            --region=*)
                AWS_REGION="${arg#*=}"
                shift
                ;;
            *)
                # Unknown option
                ;;
        esac
    done
    
    # Validate email if provided
    if [ -n "$NOTIFICATION_EMAIL" ]; then
        if [[ ! "$NOTIFICATION_EMAIL" =~ ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$ ]]; then
            error "Invalid email format: $NOTIFICATION_EMAIL"
            exit 1
        fi
        log "Notifications will be sent to: $NOTIFICATION_EMAIL"
    fi
    
    # Run deployment steps
    check_prerequisites
    deploy_infrastructure
    build_and_push_image
    test_pipeline
    
    if [ "$SKIP_GITHUB" = false ]; then
        setup_github_actions
    fi
    
    # Final summary
    cd terraform
    S3_BUCKET=$(terraform output -raw s3_bucket_name)
    MANUAL_CMD=$(terraform output -raw manual_run_command)
    cd ..
    
    log "🎉 Deployment completed successfully!"
    echo ""
    echo "📋 Summary:"
    echo "  - S3 Bucket: $S3_BUCKET"
    echo "  - Region: $AWS_REGION"
    echo "  - Schedule: 1st of each month at 00:00 UTC"
    echo "  - Estimated monthly cost: ~$0.65"
    echo ""
    echo "🔧 Manual run command:"
    echo "$MANUAL_CMD"
    echo ""
    echo "📊 Monitor:"
    echo "  - CloudWatch Logs: aws logs tail /ecs/$PROJECT_NAME --follow"
    echo "  - S3 Contents: aws s3 ls s3://$S3_BUCKET --recursive"
    echo "  - Test results: aws s3 ls s3://$S3_BUCKET/persistent/player_info/raw/"
    echo "  - Pipeline status: aws s3 ls s3://$S3_BUCKET/results/"
    echo ""
    echo "📁 Expected S3 Structure:"
    echo "  s3://$S3_BUCKET/"
    echo "  ├── persistent/player_info/raw/YYYY-MM.txt"
    echo "  └── results/YYYY-MM/completion.txt"
    echo ""
    
    if [ -n "$NOTIFICATION_EMAIL" ]; then
        warn "Don't forget to confirm your SNS email subscription!"
    fi
}

# Help function
show_help() {
    echo "Chess Glicko Pipeline Deployment Script"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --email=EMAIL     Email address for notifications"
    echo "  --region=REGION   AWS region (default: us-east-2)"
    echo "  --skip-github     Skip GitHub Actions setup"
    echo "  --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --email=admin@example.com"
    echo "  $0 --region=eu-west-1 --skip-github"
}

# Handle arguments
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    show_help
    exit 0
fi

# Run main function
main "$@"