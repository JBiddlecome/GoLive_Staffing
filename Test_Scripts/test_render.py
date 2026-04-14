import logging, traceback
from fastapi import Request
from fastapi.templating import Jinja2Templates
t = Jinja2Templates(directory='templates')
from apps.client_communication_summary.views import PROMPTS, DEFAULT_PROMPT
from starlette.datastructures import Headers
import sys

r = Request({'type': 'http', 'headers': Headers().raw})
try:
    response = t.TemplateResponse('apps/client_communication_summary.html', {'request': r, 'default_prompt': DEFAULT_PROMPT, 'prompts': PROMPTS})
    print("SUCCESS")
except Exception as e:
    print(traceback.format_exc())
    print('ERROR', e)
