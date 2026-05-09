from fastapi import FastAPI


def create_app() -> FastAPI:
    app = FastAPI()

    @app.get('/')
    async def index():
        return {'message': 'hello from h2corn'}

    return app
