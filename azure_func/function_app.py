import azure.functions as func

app = func.FunctionApp()


@app.route(route="health", methods=[func.HttpMethod.GET], auth_level=func.AuthLevel.ANONYMOUS)
def health(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("ok", status_code=200, mimetype="text/plain")

#import pipeline_t100  # --- IGNORE ---
#import manifest_t100  # --- IGNORE ---
#import db1b_timer  # --- IGNORE ---
