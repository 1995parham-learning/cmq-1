<h1 align="center">Central Messaging Queue Minus One</h1>
<h6 align="center">In-memory central messaging queue for dummies</h6>

<p align="center">
  <img alt="GitHub Actions Workflow Status" src="https://img.shields.io/github/actions/workflow/status/1995parham-learning/cmq-1/ci.yaml?style=for-the-badge&logo=github">
  <img alt="GitHub repo size" src="https://img.shields.io/github/repo-size/1995parham-learning/cmq-1?logo=github&style=for-the-badge">
  <img alt="GitHub go.mod Go version (subdirectory of monorepo)" src="https://img.shields.io/github/go-mod/go-version/1995parham-learning/cmq-1?style=for-the-badge&logo=go">
</p>

## Introduction

Sometimes you want to test your code with mocks and here is a mock for messaging queue. It tries to mimic the NATS concepts
and for sure it cannot. It doesn't have any dependencies except Go standard libraries and you can easily use it in your tests.

```go
mmq := cmq.NewMockMessageQueue[int]()

// register two different subscribing groups.
mmq.Register("s1", "numbers", 10)
mmq.Register("s2", "numbers", 10)
```
