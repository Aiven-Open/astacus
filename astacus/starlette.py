"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details

Starlette utilities.

"""

from collections.abc import Awaitable, Callable, Sequence
from pydantic.v1 import BaseModel, ValidationError
from starlette.background import BackgroundTasks
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route

import dataclasses
import inspect
import json
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
            kwargs = {}
            if body_sig is not None:
                if issubclass(body_sig.annotation, msgspec.Struct):
                    try:
                        body = await request.body()
                        if body:
                            kwargs["body"] = msgspec.json.decode(await request.body(), type=body_sig.annotation)
                        elif body_sig.default != inspect.Parameter.empty:
                            kwargs["body"] = body_sig.default
                        else:
                            raise json_http_exception(400, {"error": "missing required request body"})
                    except msgspec.ValidationError as e:
                        raise json_http_exception(400, {"error": str(e)})
                elif issubclass(body_sig.annotation, BaseModel):
                    try:
                        body = await request.body()
                        if body:
                            kwargs["body"] = body_sig.annotation.parse_obj(await request.json())
                        elif body_sig.default != inspect.Parameter.empty:
                            kwargs["body"] = body_sig.default
                        else:
                            raise json_http_exception(400, {"error": "missing required request body"})
                    except ValidationError as e:
                        raise json_http_exception(400, {"error": e.errors()})

            if "request" in signature.parameters:
                kwargs["request"] = request
            background = None
            if "background_tasks" in signature.parameters:
                background = BackgroundTasks()
                kwargs["background_tasks"] = background
            result = await func(**kwargs)
            match result:
                case msgspec.Struct():
                    return Response(msgspec.json.encode(result), media_type="application/json", background=background)
                case BaseModel():
                    return Response(result.json(), media_type="application/json", background=background)
                case None:
                    return Response(background=background)
                case _:
                    return JSONResponse(result, background=background)

        return Route(path, endpoint, methods=[method])

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


def json_http_exception(status_code: int, body: dict) -> HTTPException:
    """Create a Starlette HTTPException with a JSON body."""
    return HTTPException(status_code, json.dumps(body), headers={"Content-Type": "application/json"})


def get_query_param(request: Request, name: str) -> str:
    """Get a query parameter from a Starlette request."""
    result = request.query_params.get(name)
    if result is None:
        raise HTTPException(422, f"missing query parameter {name}")
    return result
