# infrastructure/

**Terraform** Infrastructure-as-Code (IaC) for all AWS resources.

## Resources Provisioned

| Resource | Description |
|----------|-------------|
| VPC & Subnets | Isolated network for all services |
| Amazon MSK | Managed Kafka cluster |
| Amazon S3 | Data lake buckets (bronze / silver / gold) |
| Amazon Redshift | Data warehouse cluster |
| AWS Glue | Data catalog for S3 Delta tables |
| Amazon EC2 | Airflow & Spark master nodes |
| CloudWatch Alarms | Pipeline health monitoring (`alarms.tf`) |
| IAM Roles | Least-privilege roles for each service |

## Files

| File | Purpose |
|------|---------|
| `main.tf` | Root module — calls all sub-modules |
| `variables.tf` | Input variable declarations |
| `outputs.tf` | Exported values (endpoints, ARNs) |
| `alarms.tf` | CloudWatch alarm definitions |
| `dev.tfvars` | Variable values for Dev environment |
| `staging.tfvars` | Variable values for Staging environment |
| `prod.tfvars` | Variable values for Production environment |

## Deployment

```bash
cd infrastructure/

# Initialize providers and modules
terraform init

# Preview changes
terraform plan -var-file=dev.tfvars

# Apply
terraform apply -var-file=dev.tfvars

# Destroy (careful in prod!)
terraform destroy -var-file=dev.tfvars
```

> **Note**: Never commit `.tfvars` files with secrets to version control.  
> Use AWS Secrets Manager or SSM Parameter Store for sensitive values.
