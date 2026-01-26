## Serverless Notes App

A simple AWS Serverless application showcasing how to build a full-stack solution using S3, API Gateway, AWS Lambda, and DynamoDB.

The frontend is a static HTML page hosted on Amazon S3, styled with Bootstrap, and communicates with backend APIs via API Gateway.

### Architecture
Browser
  │
  │  Static HTML / JS
  ▼
Amazon S3 (Static Website Hosting)
  │
  │  REST API calls (fetch)
  ▼
Amazon API Gateway
  │
  ▼
AWS Lambda (Python)
  │
  ▼
Amazon DynamoDB

### Features

- Static frontend hosted on S3
- Bootstrap-based responsive UI
- REST API integration using JavaScript fetch
- Create and read notes
- Fully serverless (no servers to manage)