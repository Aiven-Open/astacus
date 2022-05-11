# Some notes on the implementation

## Platform choices

- Target latest reasonable Python (3.8 or later)
    - Go was also on the table, but rohmu is compelling reason to
      use Python
- Use [pytest][pytest] for unit tests
- Use [rohmu][rohmu] for object storage interface
- Use [fastapi][fastapi] for REST API implementation(s)

[rohmu]: https://pypi.org/project/rohmu/
[fastapi]: https://fastapi.tiangolo.com
[pytest]: https://docs.pytest.org/en/latest/
