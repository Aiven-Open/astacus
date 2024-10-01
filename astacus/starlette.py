"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details

Starlette utilities.

"""

from collections.abc import Awaitable, Callable, Mapping, Sequence
from pydantic.v1 import BaseModel, ValidationError
from starlette.background import BackgroundTasks
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route

# Uncomment when we no longer support f39
# from starlette.types import ExceptionHandler
from typing import Any, TypeAlias

import dataclasses
import inspect
import msgspec.json

EndpointFunc = Callable[..., Awaitable[msgspec.Struct | dict | BaseModel | bool | None]]


@dataclasses.dataclass
class Router:
    """Build Starlette routes from python types like `msgspec.Struct` and
    `pydantic.BaseModel`.

    ```python
    import msgspec

    class RequestBody(msgspec.Struct):
        first_name: str
        last_name_name: str

    class ResponseBody(msgspec.Struct):
        full_name: str

    router = Router()
    @router.post("/full_name")
    async def full_name(request: Request, body: RequestBody) -> ResponseBody:
        return ResponseBody(full_name=f"{body.first_name} {body.last_name}")

    mount = router.mount("/api")
    ```
    """

    _routes: list[Route] = dataclasses.field(default_factory=list)

    def build_route(self, path: str, method: str, func: EndpointFunc) -> Route:
        signature = inspect.signature(func)
        body_sig = signature.parameters.get("body", None)

        async def endpoint(request: Request) -> Response:
            kwargs: dict[str, Any] = {}
            if body_sig is not None:
                kwargs["body"] = await self._parse_body(request, body_sig)

            if "request" in signature.parameters:
                kwargs["request"] = request

            background = None
            if "background_tasks" in signature.parameters:
                background = BackgroundTasks()
                kwargs["background_tasks"] = background

            result = await func(**kwargs)

            return self._parse_response(result, background=background)

        return Route(path, endpoint, methods=[method])

    async def _parse_body(self, request: Request, body_sig: inspect.Parameter) -> msgspec.Struct | BaseModel:
        if issubclass(body_sig.annotation, msgspec.Struct):
            try:
                body = await request.body()
                if body:
                    return msgspec.json.decode(await request.body(), type=body_sig.annotation)
                if body_sig.default != inspect.Parameter.empty:
                    return body_sig.default
                raise JSONHTTPException(422, {"error": "missing required request body"})
            except msgspec.ValidationError as e:
                raise JSONHTTPException(422, {"error": str(e)}) from e
        elif issubclass(body_sig.annotation, BaseModel):
            try:
                body = await request.body()
                if body:
                    return body_sig.annotation.parse_obj(await request.json())
                if body_sig.default != inspect.Parameter.empty:
                    return body_sig.default
                raise JSONHTTPException(422, {"error": "missing required request body"})
            except ValidationError as e:
                raise JSONHTTPException(422, {"error": e.errors()}) from e
        else:
            raise RuntimeError(f"unsupported body type {body_sig.annotation}")

    def _parse_response(
        self, response: msgspec.Struct | dict | BaseModel | bool | None, background: BackgroundTasks | None
    ) -> Response:
        match response:
            case msgspec.Struct():
                return Response(msgspec.json.encode(response), media_type="application/json", background=background)
            case BaseModel():
                return Response(response.json(), media_type="application/json", background=background)
            case None:
                return Response(background=background)
            case _:
                return JSONResponse(response, background=background)

    def get(self, path: str):
        def decorator(func: EndpointFunc):
            self._routes.append(self.build_route(path, "GET", func))
            return func

        return decorator

    def post(self, path: str):
        def decorator(func: EndpointFunc):
            self._routes.append(self.build_route(path, "POST", func))
            return func

        return decorator

    def put(self, path: str):
        def decorator(func: EndpointFunc):
            self._routes.append(self.build_route(path, "PUT", func))
            return func

        return decorator

    def delete(self, path: str):
        def decorator(func: EndpointFunc):
            self._routes.append(self.build_route(path, "DELETE", func))
            return func

        return decorator

    def get_routes(self) -> Sequence[Route]:
        return self._routes

    def mount(self, path: str = "") -> Mount:
        return Mount(path, routes=self.get_routes())


def get_query_param(request: Request, name: str) -> str:
    """Get a query parameter from a Starlette request."""
    result = request.query_params.get(name)
    if result is None:
        raise JSONHTTPException(422, f"missing query parameter {name}")
    return result


class JSONHTTPException(Exception):
    def __init__(self, status_code: int, body: Any) -> None:
        self.status_code = status_code
        self.body = body


def handle_json_http_exception(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, JSONHTTPException)
    return JSONResponse({"detail": exc.body}, status_code=exc.status_code)


ExceptionHandler: TypeAlias = Callable[[Request, Exception], Response]
EXCEPTION_HANDLERS: Mapping[type[Exception], ExceptionHandler] = {JSONHTTPException: handle_json_http_exception}
