# Some notes on the implementation

## Platform choices

- Target latest reasonable Python (3.7 or later, TBD)
    - Go was also on the table, but pghoard.rohmu is compelling reason to
      use Python
- Use [pytest][pytest] for unit tests
- Use pghoard.rohmu from [pghoard][pghoard] for object storage interface
- Use [fastapi][fastapi] for REST API implementation(s)

[pghoard]: https://pypi.org/project/pghoard/
[fastapi]: https://fastapi.tiangolo.com
[pytest]: https://docs.pytest.org/en/latest/
