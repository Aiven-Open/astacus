# Some notes on the implementation

## Platform choices

- Target latest reasonable Python (3.12 or later)
    - Go was also on the table, but rohmu is compelling reason to
      use Python
- Use [pytest][pytest] for unit tests
- Use [rohmu][rohmu] for object storage interface
- Use [msgspec][msgspec] for structs.  We need an efficient format because of
  the size of the backup manifests.
- Use [starlette][starlette] for REST API implementation(s).  Since we use
  `msgspec` rather than `pydantic`, there is no point using `fastapi`.

[rohmu]: https://pypi.org/project/rohmu/
[starlette]: https://www.starlette.io/
[pytest]: https://docs.pytest.org/en/latest/
[msgspec]: https://jcristharif.com/msgspec/
