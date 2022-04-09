# How these unit tests work

We write a unit test for every function. Per pytest convention, the test should be named like `test_the_function_we_are_testing()`. The test file should import operator_pvc_manager and call the function, then make an `assert`ion based on the expected outcome of the test. A single function can have any number of test cases and assertions.

## How to test locally

From the repo root:

```
python3 setup.py pytest --addopts "-n 4 --cov-report html --cov=operator_pvc_manager tests/"
```

Pytest runs tests in parallel so we really have to avoid any side effects in our functions, hence the mocking.

HTML coverage report shows up in 'htmlcov/'.

## How the fixtures work

Pytest lets you define fixtures, which are repetitive setup steps that you'll end up using in multiple tests. Most of the fixures are defined in `conftest.py`, which pytest automatically imports.

Since operator-pvc-manager is just some minimal logic in front of a bunch of kubernetes/AWS API calls, in order to unit test we have to mock a lot of API calls. These are **unit** tests, not integration tests, so we don't want to have to run them against a real live Kube cluster, searching for real live CloudTrail events. The tests would take forever and make a bunch of assumptions about the environment.

The mocking strategy is to use MagicMock.

### MagicMock

This helps you make fake objects that behave like the real thing.

It can easily fake return values:

```
>>> pvc = mock.MagicMock()
>>> pvc.metadata.annotations.get.return_value = 'my-cool-sts'
>>> pvc.metadata.annotations.get('pvc-manager/statefulset')
'service-foo-bar'
>>> pvc.metadata.annotations.get('anything, actually')
'service-foo-bar'
```

It can pretend to contain dictionaries and strings:

```
>>> pvc.spec.resources.requests = {'storage':'12Gi'}
>>> pvc.spec.resources.requests['storage']
'12Gi'
>>> pvc.metadata.namespace = 'default'
>>> pvc.metadata.namespace
'default'
```

This is nothing you couldn't accomplish by just building some mock classes, it just saves a bunch of keystrokes and lets you change properties on-the-fly.

## How we substitute the mocked object

When operator-pvc-manager is being ran directly (when `__name__ == "__main__"`), we set up real API objects like `v1 = CoreV1Api()`, but when we `import` it, none of that setup happens.

We can insert our mocked API object into the imported module, because a module is just another object, and the global namespace of the imported module is available to us. e.g.

```
>>> import operator_pvc_manager
>>> fake_v1 = MagicMock()
>>> operator_pvc_manager.v1 = fake_v1
```

The fixtures create the fake objects, which we side-load as needed during the unit tests.
