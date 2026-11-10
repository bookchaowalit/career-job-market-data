import { defineRailway, github, project, service } from "railway/iac";
// Railway IaC. Replaces railway.toml / railway.json, which Railway stops reading on 2026-12-01.
// Build, healthcheck, start command, cron and restart policy live on the service; deploys do not read this file.
// Check drift: npm install railway in a scratch parent directory (not this repo), railway link, railway config plan.
// This repository manages only its own resources in the environment. Other
// repositories export their own partial name.
// See https://docs.railway.com/infrastructure-as-code#multi-repo-projects
export const partial = "book-job-data-api";
export default defineRailway(() => {
  const bookJobDataApi = service("book-job-data-api", {
    source: github("bookchaowalit/career-job-market-data", { branch: "main", checkSuites: true }),
    build: { buildEnvironment: "V3", builder: "DOCKERFILE", dockerfilePath: "Dockerfile" },
    healthcheck: "/healthz",
    healthcheckTimeout: 60,
    replicas: { "asia-southeast1-eqsg3a": 1 },
    deploy: { restartPolicyMaxRetries: 3 },
  });

  return project("book-job-data", {
    variables: { managed: false },
    resources: [bookJobDataApi],
  });
});
