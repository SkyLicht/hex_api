import time


from fastapi import FastAPI, Depends, HTTPException, Request

from sqlalchemy.exc import SQLAlchemyError
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

from core.api.routes.layout import line_endpoint
from core.api.routes.planner import work_plan_endpoint, platform_endpoint
from core.api.routes.statistics import ppid_endpoint
from core.db.ie_tool_db import IETOOLDBConnection

app = FastAPI()
# Allow CORS for localhost:3000
app.add_middleware(
    CORSMiddleware,
    # allow_origins=["http://localhost:3000","http://10.13.33.46:3000","http://localhost:8000"],  # Adjust origins as needed
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

@app.middleware("http")
async def db_session_middleware(request: Request, call_next):


    db = IETOOLDBConnection().get_session()
    # get user
    start_time = time.time()  # Record the start time of the requests


    try:
        request.state.db = db
        response = await call_next(request)

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return response

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """
    This handler intercepts any HTTPException raised anywhere in the app.
    You can still raise custom HTTPExceptions in your routes or repositories,
    and they will be handled here.
    """

    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )


@app.exception_handler(SQLAlchemyError)
async def sqlalchemy_error_handler(request: Request, exc: SQLAlchemyError):
    """
    This handler intercepts any SQLAlchemy error (e.g., IntegrityError, OperationalError).
    """

    return JSONResponse(
        status_code=400,
        content={"detail": "A database error occurred."}
    )


@app.exception_handler(PermissionError)
async def permission_error_handler(request: Request, exc: PermissionError):

    return JSONResponse(
        status_code=403,
        content={"detail": "Permission denied."}
    )


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):


    return JSONResponse(
        status_code=422,
        content={"detail": str(exc)}
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):

    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )



@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """
    This is a 'catch-all' handler. It will catch any exception that isn't
    handled by the more specific exception handlers above.
    """

    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected error occurred."}
    )


@app.exception_handler(TypeError)
async def type_error_handler(request: Request, exc: TypeError):

    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected error occurred."}
    )


app.include_router(
    prefix='/api/v1',
    router= work_plan_endpoint.router
)

app.include_router(
    prefix='/api/v1',
    router= platform_endpoint.router
)

app.include_router(
    prefix='/api/v1',
    router= line_endpoint.router
)
app.include_router(
    prefix='/api/v1',
    router= ppid_endpoint.router
)
@app.get("/")
async def read_root():
    return {"Hello": "World"}
