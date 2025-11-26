from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi import APIRouter, File, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.concurrency import run_in_threadpool

from golive_profile_creator import run_profile_creator

router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _render_page(request: Request, notice: str | None = None, error: str | None = None) -> HTMLResponse:
    return templates.TemplateResponse(
        "apps/golive_profile_creator.html",
        {
            "request": request,
            "notice": notice,
            "error": error,
        },
    )


@router.get("", response_class=HTMLResponse)
async def golive_profile_creator_page(request: Request) -> HTMLResponse:
    """Render the upload form for the GoLive profile creator."""
    return _render_page(request)


@router.post("", response_class=HTMLResponse)
async def golive_profile_creator_upload(request: Request, data_file: UploadFile = File(...)) -> HTMLResponse:
    """Accept an upload and kick off the Playwright automation in a threadpool."""
    file_bytes = await data_file.read()
    if not file_bytes:
        return _render_page(request, error="The uploaded file was empty. Please upload a CSV or Excel file.")

    suffix = Path(data_file.filename or "upload").suffix or ".csv"
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp_path = Path(tmp_file.name)
    tmp_file.close()

    try:
        tmp_path.write_bytes(file_bytes)
        await run_in_threadpool(run_profile_creator, tmp_path)
    except Exception as exc:  # noqa: BLE001 - surface readable error to the page
        return _render_page(request, error=f"Automation failed: {exc}")
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return _render_page(request, notice="Automation completed successfully. Check the logs for details.")
