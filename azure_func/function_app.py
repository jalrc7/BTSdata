import azure.functions as func

app = func.FunctionApp()

import pipeline_t100 
import count_rowst100 
import manifest_t100
import db1b_timer
import download_t100
import list_t100

@app.route(route="ping", methods=[func.HttpMethod.GET], auth_level=func.AuthLevel.ANONYMOUS)
def ping(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("pong", status_code=200, mimetype="text/plain")