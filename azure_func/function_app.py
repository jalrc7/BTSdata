import azure.functions as func

app = func.FunctionApp()


@app.route(route="ping", methods=[func.HttpMethod.GET], auth_level=func.AuthLevel.ANONYMOUS)
def ping(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("pong", status_code=200, mimetype="text/plain")

#import pipeline_t100  # --- IGNORE ---
#import manifest_t100  # --- IGNORE ---
#import db1b_timer  # --- IGNORE ---
import download_t100  # --- IGNORE ---
import list_t100  # --- IGNORE ---
