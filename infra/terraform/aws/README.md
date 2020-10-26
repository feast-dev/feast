# Terraform config for feast on AWS

Uses terraform 0.12

1. Run `aws emr create-default-roles` once.

2. User has an option to use existing vpc or to create one for feast deployment.

```
# to use existing vpc
cd with-existing-vpc

# to create new vpc for feast
cd with-new-vpc
```

3. Create a tfvars file, e.g. `my.tfvars` and set name_prefix:

```
name_prefix = "my-feast"
region      = "us-east-1"
```

4. To use existing vpc add below mentioned additional variables to `my.tfvars` file

   Note: _the tags in `subnet_filter_tag` should match the tags associated with subnets in your aws account. Please make sure atleast two subnets are selected, as Kafka is set up with two broker nodes_.

   Note: _if `subnet_filter_tag={}` is set to empty map as shown, all the subnets(both private and public) will be selected_.

```
vpc_id = <your_vpc_id>
subnet_filter_tag = {
  <tag_key1> = <tag_value1>
  <tag_key2> = <tag_value2>
  ...
}
```

5. Configure tf state backend, e.g.:

```
terraform {
  backend "s3" {
    bucket         = "my-terraform-state-bucket"
    key            = "clusters/my-feast-test"
    region         = "us-west-2"
    dynamodb_table = "terraform-state-lock"
    encrypt        = true
  }
}
```

6. Use `terraform apply -var-file="my.tfvars"` to deploy.

7. For more configuration options look at the available variables in `variables.tf` files in folders `with-existing-vpc` and `with-new-vpc`
