# Camera Streaming Fix TODO

## Issues Fixed
- [x] USB camera connects but doesn't stream - Changed USB cameras to use OpenCV instead of FFmpeg
- [x] Stop camera and restart doesn't work - Updated stop() method to properly clean up resources
- [x] AttributeError: 'FFmpegWorker' object has no attribute 'stream_thread' - Added missing attributes in __init__

## Changes Made
1. Modified FFmpegWorker.__init__ to use OpenCV for USB cameras (int source) and FFmpeg for IP cameras (string source)
2. Updated stop() method to properly clean up all resources:
   - Terminate FFmpeg processes for IP cameras
   - Stop capture threads for USB cameras
   - Release OpenCV VideoCapture objects
3. USB cameras now use _init_capture() and _start_capture_thread() for streaming
4. IP cameras continue to use FFmpeg with _start_ffmpeg_process()
5. Added missing USB camera attributes: stream_thread, stream_running, cap

## Migration to FastAPI
- [x] Replace Flask imports with FastAPI imports (FastAPI, Request, Response, HTTPException, StreamingResponse, JSONResponse, HTMLResponse, Jinja2Templates, uvicorn)
- [x] Change app initialization from Flask to FastAPI
- [x] Add Jinja2Templates for HTML rendering
- [x] Convert all Flask route decorators to FastAPI (@app.get, @app.post with path parameters)
- [x] Replace Flask jsonify with FastAPI JSONResponse
- [x] Replace Flask Response with FastAPI StreamingResponse for video streams
- [x] Make all endpoints async where appropriate
- [x] Convert Flask request handling to FastAPI Request objects
- [x] Update index route to use templates.TemplateResponse
- [x] Replace Flask app.run() with uvicorn.run()

## Testing
- [ ] Test USB camera streaming after changes
- [ ] Test stop/restart functionality for USB cameras
- [ ] Test IP camera streaming still works
- [ ] Test stop/restart for IP cameras
- [ ] Test FastAPI migration - all endpoints functional
- [ ] Test UI interactions with new FastAPI backend
