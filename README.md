<h1 align="center">In-memory central messaging queue for dummies</h1>

## Introduction

Sometimes you want to test your code with mocks and here is a mock for messaging queue. It tries to mimic the NATS concepts
and for sure it cannot. It doesn't have any dependencies except Go standard libraries and you can easily use it in your tests.

```go
mmq := cmq.NewMockMessageQueue[int]()

# register two different subscribing groups.
mmq.Register("s1", "numbers", 10)
mmq.Register("s2", "numbers", 10)
```
