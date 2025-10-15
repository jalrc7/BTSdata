import azure.functions as func

app = func.FunctionApp()

@app.route(route="ping", methods=[func.HttpMethod.GET], auth_level=func.AuthLevel.ANONYMOUS)
def ping(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("pong", status_code=200, mimetype="text/plain")