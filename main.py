import cv2
import time
import threading
import os
import re
import configparser
from queue import Queue
from datetime import datetime
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import subprocess
import numpy as np
import shutil
from onvif import ONVIFCamera
import json
import asyncio
from aiortc import RTCPeerConnection, RTCSessionDescription
from aiortc.contrib.media import MediaRelay
import uvicorn

# Global WebRTC streamers
webrtc_streamers = {}

# ==========================
# WebRTC Streaming
# ==========================
class WebRTCStreamer:
    def __init__(self, cam_id):
        self.cam_id = cam_id
        self.relay = MediaRelay()
        self.pc = None
        self.track = None
        self.ffmpeg_process = None
        self.rtp_port = 5004  # Base RTP port, increment for multiple cameras

        # Bitrate ladder: 4 Mbps / 2 Mbps / 800 kbps
        self.bitrate_ladder = [4000000, 2000000, 800000]  # in bps
        self.current_bitrate_index = 0
        self.min_bitrate = 800000  # 800 kbps
        self.max_bitrate = 4000000  # 4 Mbps

        # Network stats
        self.rtt = 0
        self.packet_loss = 0

    async def create_offer(self):
        """Create WebRTC offer for camera streaming"""
        cam = camera_manager.get_camera(self.cam_id)
        if not cam:
            return None

        # Create RTCPeerConnection
        self.pc = RTCPeerConnection()

        # Start FFmpeg RTP output
        self._start_ffmpeg_rtp()

        # Create RTP track from FFmpeg output
        # For simplicity, we'll use a placeholder - in production, bridge RTP to WebRTC
        offer = await self.pc.createOffer()
        await self.pc.setLocalDescription(offer)

        return {
            "sdp": self.pc.localDescription.sdp,
            "type": self.pc.localDescription.type,
            "bitrate_ladder": self.bitrate_ladder,
            "current_bitrate": self.bitrate_ladder[self.current_bitrate_index]
        }

    async def handle_answer(self, answer_sdp):
        """Handle WebRTC answer from client"""
        if self.pc:
            answer = RTCSessionDescription(sdp=answer_sdp, type="answer")
            await self.pc.setRemoteDescription(answer)

    def _start_ffmpeg_rtp(self):
        """Start FFmpeg to output RTP for WebRTC"""
        if not check_ffmpeg():
            print(f"FFmpeg not available for WebRTC streaming of {self.cam_id}")
            return

        cam = camera_manager.get_camera(self.cam_id)
        if not cam:
            return

        # FFmpeg command for RTP output with bitrate control
        cmd = [
            'ffmpeg',
            '-f', 'rawvideo',
            '-pixel_format', 'rgb24',
            '-video_size', f'{cam.width}x{cam.height}',
            '-framerate', str(cam.fps),
            '-i', 'pipe:0',  # Read from stdin (would need to pipe frames)
            '-c:v', 'libx264',
            '-b:v', f'{self.bitrate_ladder[self.current_bitrate_index]}',
            '-maxrate', f'{self.max_bitrate}',
            '-bufsize', f'{self.max_bitrate * 2}',
            '-f', 'rtp',
            f'rtp://127.0.0.1:{self.rtp_port}'
        ]

        # Note: In production, this would need to be integrated with the frame pipeline
        # For now, this is a placeholder structure

    def update_bitrate(self, target_bitrate):
        """Update bitrate based on network conditions"""
        # Find closest bitrate in ladder
        closest_index = min(range(len(self.bitrate_ladder)),
                           key=lambda i: abs(self.bitrate_ladder[i] - target_bitrate))
        self.current_bitrate_index = closest_index

        # Restart FFmpeg with new bitrate
        if self.ffmpeg_process:
            self.ffmpeg_process.terminate()
            self.ffmpeg_process.wait()
        self._start_ffmpeg_rtp()

    def update_network_stats(self, rtt, packet_loss):
        """Update network statistics and adjust bitrate if needed"""
        self.rtt = rtt
        self.packet_loss = packet_loss

        # Simple adaptation logic: reduce bitrate if high packet loss or RTT
        if packet_loss > 0.05 or rtt > 200:  # 5% loss or 200ms RTT
            if self.current_bitrate_index < len(self.bitrate_ladder) - 1:
                self.current_bitrate_index += 1
                self.update_bitrate(self.bitrate_ladder[self.current_bitrate_index])
        elif packet_loss < 0.01 and rtt < 100:  # Good conditions
            if self.current_bitrate_index > 0:
                self.current_bitrate_index -= 1
                self.update_bitrate(self.bitrate_ladder[self.current_bitrate_index])

    def stop(self):
        """Stop WebRTC streaming"""
        if self.ffmpeg_process:
            self.ffmpeg_process.terminate()
            self.ffmpeg_process.wait()
        if self.pc:
            asyncio.run(self.pc.close())

app = FastAPI()
templates = Jinja2Templates(directory="templates")

RECORD_DIR = "recordings"
CONFIG_FILE = "ip_configuration.ini"
os.makedirs(RECORD_DIR, exist_ok=True)

def load_config():
    config = configparser.ConfigParser()
    if os.path.exists(CONFIG_FILE):
        config.read(CONFIG_FILE)
    return config

def check_ffmpeg():
    """Check if FFmpeg and ffprobe are installed and available"""
    # Check for local FFmpeg first
    if os.path.exists('./ffmpeg-8.0.1-essentials_build/bin/ffmpeg.exe') and os.path.exists('./ffmpeg-8.0.1-essentials_build/bin/ffprobe.exe'):
        return True
    return shutil.which('ffmpeg') is not None and shutil.which('ffprobe') is not None

# ==========================
# Camera Discovery
# ==========================
def discover_usb_cameras(max_devices=10):
    cameras = []
    for i in range(max_devices):
        try:
            cap = cv2.VideoCapture(i)
            if cap.isOpened():
                ret, _ = cap.read()
                if ret:
                    cameras.append({
                        "id": f"usb_{i}",
                        "type": "usb",
                        "source": i
                    })
                cap.release()
        except Exception as e:
            print(f"Error checking camera {i}: {e}")
            continue
    return cameras



def load_ip_cameras():
    """Load IP cameras from config with optional ONVIF properties"""
    config = load_config()
    cameras = []
    for section in config.sections():
        if section.startswith('camera'):
            cam_id = config.get(section, 'id', fallback=section)
            url = config.get(section, 'url', fallback='')
            if url:
                camera = {
                    "id": cam_id,
                    "type": "ip",
                    "source": url
                }
                # Add optional ONVIF properties
                onvif_host = config.get(section, 'onvif_host', fallback='')
                onvif_port = config.getint(section, 'onvif_port', fallback=80)
                onvif_username = config.get(section, 'onvif_username', fallback='')
                onvif_password = config.get(section, 'onvif_password', fallback='')
                if onvif_host:
                    camera.update({
                        "onvif_host": onvif_host,
                        "onvif_port": onvif_port,
                        "onvif_username": onvif_username,
                        "onvif_password": onvif_password,
                        "ptz_supported": False  # Will check later
                    })
                cameras.append(camera)
    return cameras


# ==========================
# FFmpeg Worker
# ==========================
class FFmpegWorker:
    def __init__(self, cam_id, source, cam_info=None):
        self.cam_id = cam_id
        self.source = source
        self.cam_info = cam_info or {}
        self.start_time = datetime.now()
        self.ready = False
        self.error_message = None
        self.running = True
        self.recording = False
        self.record_filename = None
        self.frame = None
        self.lock = threading.Lock()
        self.stream_process = None
        self.frame_reader_thread = None
        self.frame_reader_running = False
        # USB camera specific attributes
        self.stream_thread = None
        self.stream_running = False
        self.cap = None
        self.video_writer = None

        # ONVIF setup
        self.onvif_camera = None
        self.ptz_service = None
        self.ptz_supported = False
        self.onvif_profiles = {}
        self._setup_onvif()

        # Determine input format and arg
        if isinstance(source, int):
            # USB camera
            self.input_format = 'dshow'
            self.input_arg = f'video={source}'
        else:
            # IP camera
            self.input_format = None
            self.input_arg = source

        # Determine if USB or IP camera
        if isinstance(source, int):
            # USB camera - use OpenCV
            self._init_capture()
            if self.ready:
                self._start_capture_thread()
                print(f"USB Camera {cam_id} initialized: {self.width}x{self.height} @ {self.fps}fps")
            else:
                return
        else:
            # IP camera - use FFmpeg
            # Probe metadata
            self.fps, self.width, self.height = self._probe_metadata()
            if self.fps is None:
                self.error_message = f"Failed to probe metadata for {cam_id}"
                return

            self.ready = True
            print(f"IP Camera {cam_id} initialized: {self.width}x{self.height} @ {self.fps}fps")

            # Start FFmpeg process
            self._start_ffmpeg_process()

            # Start restart monitoring
            self._start_restart_monitor()

    def _start_restart_monitor(self):
        """Start monitoring thread for FFmpeg restart on exit/freeze"""
        self.monitor_thread = threading.Thread(target=self._monitor_ffmpeg, daemon=True)
        self.monitor_thread.start()

    def _monitor_ffmpeg(self):
        """Monitor FFmpeg process and restart if it exits or freezes"""
        while self.running:
            if self.stream_process:
                if self.stream_process.poll() is not None:
                    print(f"FFmpeg process for {self.cam_id} exited, restarting...")
                    self._start_ffmpeg_process()
                else:
                    # Check if process is frozen (no output for too long)
                    # This is a simple check - in production, you might want more sophisticated monitoring
                    pass
            time.sleep(5)  # Check every 5 seconds

    def _setup_onvif(self):
        """Setup ONVIF camera and PTZ service if available"""
        if self.cam_info.get("type") == "onvif":
            try:
                self.onvif_camera = ONVIFCamera(
                    self.cam_info["onvif_host"],
                    self.cam_info["onvif_port"],
                    self.cam_info["onvif_username"],
                    self.cam_info["onvif_password"]
                )
                self.ptz_service = self.onvif_camera.create_ptz_service()
                # Check if PTZ is supported
                self.ptz_supported = self._check_ptz_support()
                if self.ptz_supported:
                    self.onvif_profiles = self._cache_onvif_profiles()
                    print(f"ONVIF PTZ supported for camera {self.cam_id}")
                else:
                    print(f"ONVIF PTZ not supported for camera {self.cam_id}")
            except Exception as e:
                print(f"Failed to setup ONVIF for camera {self.cam_id}: {e}")
                self.ptz_supported = False

    def _check_ptz_support(self):
        """Check if PTZ is supported by querying capabilities"""
        try:
            capabilities = self.onvif_camera.devicemgmt.GetCapabilities()
            return hasattr(capabilities, 'PTZ') and capabilities.PTZ is not None
        except Exception as e:
            print(f"Error checking PTZ support for {self.cam_id}: {e}")
            return False

    def _cache_onvif_profiles(self):
        """Cache ONVIF profiles to avoid repeated network calls"""
        try:
            media_service = self.onvif_camera.create_media_service()
            profiles = media_service.GetProfiles()
            profile_dict = {}
            for profile in profiles:
                profile_dict[profile.token] = profile
            return profile_dict
        except Exception as e:
            print(f"Error caching ONVIF profiles for {self.cam_id}: {e}")
            return {}

    def _probe_metadata(self):
        """Use ffprobe to get stream metadata"""
        try:
            ffprobe_path = './ffmpeg-8.0.1-essentials_build/bin/ffprobe.exe' if os.path.exists('./ffmpeg-8.0.1-essentials_build/bin/ffprobe.exe') else 'ffprobe'
            cmd = [ffprobe_path, '-v', 'quiet', '-print_format', 'json', '-show_streams']
            if self.input_format:
                cmd.extend(['-f', self.input_format])
            cmd.append(self.input_arg)

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            import json
            data = json.loads(result.stdout)
            if 'streams' in data and data['streams']:
                stream = data['streams'][0]
                width = int(stream.get('width', 1280))
                height = int(stream.get('height', 720))
                fps_str = stream.get('r_frame_rate', '30/1')
                fps = eval(fps_str) if '/' in fps_str else float(fps_str)
                return fps, width, height
            else:
                # Fallback: try to get basic info or use defaults
                print(f"No streams found in ffprobe output for {self.cam_id}, using defaults")
                return 30.0, 1280, 720
        except Exception as e:
            print(f"Failed to probe metadata for {self.cam_id}: {e}")
            return None, None, None

    def _start_frame_reader_thread(self):
        """Start the frame reader thread"""
        if self.frame_reader_thread is None or not self.frame_reader_thread.is_alive():
            self.frame_reader_running = True
            self.frame_reader_thread = threading.Thread(target=self._frame_reader_loop, daemon=True)
            self.frame_reader_thread.start()

    def _frame_reader_loop(self):
        """Read raw frames from FFmpeg stdout, convert to numpy, add overlays"""
        frame_size = self.width * self.height * 3  # RGB24
        while self.frame_reader_running and self.running:
            try:
                if self.stream_process and self.stream_process.stdout:
                    raw_data = self.stream_process.stdout.read(frame_size)
                    if len(raw_data) == frame_size:
                        # Convert to numpy array
                        frame = np.frombuffer(raw_data, dtype=np.uint8).reshape((self.height, self.width, 3))
                        # Convert RGB to BGR for OpenCV
                        frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)

                        # Add overlays
                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        cv2.putText(frame, f"LIVE - {current_time}", (10, 30),
                                  cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
                        cv2.putText(frame, f"Camera: {self.cam_id}", (10, frame.shape[0] - 10),
                                  cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)

                        with self.lock:
                            self.frame = frame
                    else:
                        time.sleep(0.01)
                else:
                    time.sleep(0.1)
            except Exception as e:
                print(f"Error reading frame for {self.cam_id}: {e}")
                time.sleep(0.1)

    def _init_capture(self):
        """Initialize camera capture"""
        try:
            self.cap = cv2.VideoCapture(self.source)
            if not self.cap.isOpened():
                self.error_message = f"Failed to open camera {self.cam_id}"
                return

            # Get camera properties
            self.fps = self.cap.get(cv2.CAP_PROP_FPS) or 30.0
            self.width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH)) or 1280
            self.height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT)) or 720

            self.ready = True
            print(f"Camera {self.cam_id} initialized: {self.width}x{self.height} @ {self.fps}fps")
        except Exception as e:
            self.error_message = f"Error initializing camera {self.cam_id}: {str(e)}"
            print(self.error_message)

    def _start_capture_thread(self):
        """Start the frame capture thread"""
        if not hasattr(self, 'stream_thread'):
            self.stream_thread = None
        if not hasattr(self, 'stream_running'):
            self.stream_running = False
        if not hasattr(self, 'cap'):
            self.cap = None
        if self.stream_thread is None or not self.stream_thread.is_alive():
            self.stream_running = True
            self.stream_thread = threading.Thread(target=self._capture_loop, daemon=True)
            self.stream_thread.start()

    def _capture_loop(self):
        """Continuously capture frames from the camera"""
        while self.stream_running and self.running:
            try:
                ret, frame = self.cap.read()
                if ret:
                    # Add timestamp overlay
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    cv2.putText(frame, f"LIVE - {current_time}", (10, 30),
                              cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
                    cv2.putText(frame, f"Camera: {self.cam_id}", (10, frame.shape[0] - 10),
                              cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)

                    with self.lock:
                        self.frame = frame

                    # Handle recording if active
                    if self.recording and self.record_filename and self.video_writer:
                        self.video_writer.write(frame)

                else:
                    print(f"Failed to read frame from camera {self.cam_id}")
                    time.sleep(0.1)
            except Exception as e:
                print(f"Error capturing frame from camera {self.cam_id}: {e}")
                time.sleep(0.1)

        print(f"Capture loop stopped for camera {self.cam_id}")

    def _start_monitor(self):
        """Start the monitor thread for restart on failure"""
        if self.monitor_thread is None or not self.monitor_thread.is_alive():
            self.monitor_running = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()

    def _monitor_loop(self):
        """Monitor FFmpeg process and restart on failure"""
        while self.monitor_running and self.running:
            if self.stream_process and self.stream_process.poll() is not None:
                print(f"FFmpeg process for {self.cam_id} has failed, restarting...")
                self._start_ffmpeg_process()
            time.sleep(5)  # Check every 5 seconds

    def _get_metadata(self):
        """Use ffprobe to get stream metadata"""
        try:
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams'
            ]
            if self.input_format:
                cmd.extend(['-f', self.input_format])
            cmd.append(self.input_arg)

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            import json
            data = json.loads(result.stdout)
            stream = data['streams'][0]
            width = int(stream['width'])
            height = int(stream['height'])
            fps = eval(stream['r_frame_rate'])  # e.g., "30/1" -> 30.0
            return fps, width, height
        except Exception as e:
            print(f"Failed to get metadata for {self.cam_id}: {e}")
            return 30.0, 1280, 720  # Defaults

    def _start_ffmpeg_process(self):
        """Start the FFmpeg subprocess for raw frame output and optional recording"""
        if not check_ffmpeg():
            self.error_message = "FFmpeg is not installed or not in PATH. Please install FFmpeg to use camera streaming."
            print(f"FFmpeg not found for {self.cam_id}")
            return

        ffmpeg_path = './ffmpeg-8.0.1-essentials_build/bin/ffmpeg.exe' if os.path.exists('./ffmpeg-8.0.1-essentials_build/bin/ffmpeg.exe') else 'ffmpeg'
        cmd = [ffmpeg_path]
        if self.input_format:
            cmd.extend(['-f', self.input_format])
        cmd.extend(['-i', self.input_arg])

        # Use tee to output to recording (if enabled) and rawvideo to pipe
        tee_outputs = ""
        if self.recording:
            # Use segmented MP4 with faststart for safe recording
            tee_outputs += f"[f=segment:segment_time=10:segment_format=mp4:segment_list=NUL]{self.record_filename}.%03d.mp4|"
        tee_outputs += "[f=rawvideo:pix_fmt=rgb24]pipe:1"

        cmd.extend(['-f', 'tee', '-map', '0:v', tee_outputs])

        # Add faststart flag for better streaming compatibility
        if self.recording:
            cmd.extend(['-movflags', '+faststart'])

        try:
            self.stream_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0)
            print(f"FFmpeg process started for {self.cam_id}" + (" with recording" if self.recording else ""))

            # Start frame reader thread
            self._start_frame_reader_thread()
        except FileNotFoundError:
            self.error_message = "FFmpeg executable not found. Please install FFmpeg."
            print(f"FFmpeg not found for {self.cam_id}")



    def start_recording(self):
        if not self.recording:
            current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            self.record_filename = f"{RECORD_DIR}/{self.cam_id}_{current_date}.mp4"
            self.recording = True

            # For USB cameras, initialize VideoWriter
            if isinstance(self.source, int):
                fourcc = cv2.VideoWriter_fourcc(*'mp4v')
                self.video_writer = cv2.VideoWriter(self.record_filename, fourcc, self.fps, (self.width, self.height))
                print(f"Recording started for USB camera {self.cam_id}: {self.record_filename}")
            else:
                # For IP cameras, restart FFmpeg process with recording enabled
                if self.stream_process:
                    self.stream_process.terminate()
                    self.stream_process.wait()
                self._start_ffmpeg_process()
            return self.record_filename

    def stop_recording(self):
        if self.recording:
            self.recording = False

            # For USB cameras, release VideoWriter
            if isinstance(self.source, int) and self.video_writer:
                self.video_writer.release()
                self.video_writer = None
                print(f"Recording stopped for USB camera {self.cam_id}: {self.record_filename}")

            self.record_filename = None

            # For IP cameras, restart FFmpeg process without recording
            if self.stream_process:
                self.stream_process.terminate()
                self.stream_process.wait()
            self._start_ffmpeg_process()

    def get_frame(self):
        with self.lock:
            return self.frame.copy() if hasattr(self, 'frame') and self.frame is not None else None

    def uptime(self):
        return (datetime.now() - self.start_time).total_seconds()

    def pan(self, direction):
        """Pan the camera left or right"""
        if not self.ptz_supported or not self.ptz_service:
            print(f"PTZ not supported for camera {self.cam_id}")
            return False

        try:
            # Get current status
            status = self.ptz_service.GetStatus({'ProfileToken': list(self.onvif_profiles.keys())[0]})
            current_position = status.Position

            # Define pan speed and movement
            pan_speed = 0.5
            if direction == 'left':
                pan_velocity = -pan_speed
            elif direction == 'right':
                pan_velocity = pan_speed
            else:
                return False

            # Create continuous move request
            request = self.ptz_service.create_type('ContinuousMove')
            request.ProfileToken = list(self.onvif_profiles.keys())[0]
            request.Velocity = {
                'PanTilt': {
                    'x': pan_velocity,
                    'y': 0.0
                },
                'Zoom': {
                    'x': 0.0
                }
            }

            self.ptz_service.ContinuousMove(request)
            print(f"Panning {direction} for camera {self.cam_id}")
            return True
        except Exception as e:
            print(f"Error panning camera {self.cam_id}: {e}")
            return False

    def tilt(self, direction):
        """Tilt the camera up or down"""
        if not self.ptz_supported or not self.ptz_service:
            print(f"PTZ not supported for camera {self.cam_id}")
            return False

        try:
            # Get current status
            status = self.ptz_service.GetStatus({'ProfileToken': list(self.onvif_profiles.keys())[0]})
            current_position = status.Position

            # Define tilt speed and movement
            tilt_speed = 0.5
            if direction == 'up':
                tilt_velocity = tilt_speed
            elif direction == 'down':
                tilt_velocity = -tilt_speed
            else:
                return False

            # Create continuous move request
            request = self.ptz_service.create_type('ContinuousMove')
            request.ProfileToken = list(self.onvif_profiles.keys())[0]
            request.Velocity = {
                'PanTilt': {
                    'x': 0.0,
                    'y': tilt_velocity
                },
                'Zoom': {
                    'x': 0.0
                }
            }

            self.ptz_service.ContinuousMove(request)
            print(f"Tilting {direction} for camera {self.cam_id}")
            return True
        except Exception as e:
            print(f"Error tilting camera {self.cam_id}: {e}")
            return False

    def zoom(self, direction):
        """Zoom the camera in or out"""
        if not self.ptz_supported or not self.ptz_service:
            print(f"PTZ not supported for camera {self.cam_id}")
            return False

        try:
            # Define zoom speed and movement
            zoom_speed = 0.5
            if direction == 'in':
                zoom_velocity = zoom_speed
            elif direction == 'out':
                zoom_velocity = -zoom_speed
            else:
                return False

            # Create continuous move request
            request = self.ptz_service.create_type('ContinuousMove')
            request.ProfileToken = list(self.onvif_profiles.keys())[0]
            request.Velocity = {
                'PanTilt': {
                    'x': 0.0,
                    'y': 0.0
                },
                'Zoom': {
                    'x': zoom_velocity
                }
            }

            self.ptz_service.ContinuousMove(request)
            print(f"Zooming {direction} for camera {self.cam_id}")
            return True
        except Exception as e:
            print(f"Error zooming camera {self.cam_id}: {e}")
            return False

    def stop_ptz(self):
        """Stop PTZ movement"""
        if not self.ptz_supported or not self.ptz_service:
            print(f"PTZ not supported for camera {self.cam_id}")
            return False

        try:
            # Create stop request
            request = self.ptz_service.create_type('Stop')
            request.ProfileToken = list(self.onvif_profiles.keys())[0]
            request.PanTilt = True
            request.Zoom = True

            self.ptz_service.Stop(request)
            print(f"Stopping PTZ movement for camera {self.cam_id}")
            return True
        except Exception as e:
            print(f"Error stopping PTZ for camera {self.cam_id}: {e}")
            return False



    def stop(self):
        self.stop_recording()
        self.running = False
        self.frame_reader_running = False
        # Stop FFmpeg process if running (for IP cameras)
        if self.stream_process:
            self.stream_process.terminate()
            self.stream_process.wait()
        # Stop capture thread if running (for USB cameras)
        if hasattr(self, 'stream_thread') and self.stream_thread and self.stream_thread.is_alive():
            self.stream_running = False
            self.stream_thread.join(timeout=1.0)
        # Release OpenCV capture if exists (for USB cameras)
        if hasattr(self, 'cap') and self.cap:
            self.cap.release()


# ==========================
# Camera Manager
# ==========================
class CameraManager:
    def __init__(self):
        self.available = []
        self.active = {}
        self.camera_health = {}  # Track health status of cameras
        self.health_check_thread = None
        self.health_check_running = False

    def discover(self):
        self.available = discover_usb_cameras() + load_ip_cameras()
        # Initialize health status for all cameras
        for cam in self.available:
            self.camera_health[cam["id"]] = {
                "status": "unknown",
                "last_checked": datetime.now(),
                "connected": False,
                "error_message": None
            }
        # All cameras are now available for user selection
        print(f"Discovered {len(self.available)} cameras: {[cam['id'] for cam in self.available]}")
        self.start_health_monitoring()

    def start_health_monitoring(self):
        """Start background health monitoring for all cameras"""
        if self.health_check_thread is None or not self.health_check_thread.is_alive():
            self.health_check_running = True
            self.health_check_thread = threading.Thread(target=self._health_check_loop, daemon=True)
            self.health_check_thread.start()

    def _health_check_loop(self):
        """Background loop to check camera health"""
        while self.health_check_running:
            for cam_id, health_info in self.camera_health.items():
                self._check_camera_health(cam_id)
            time.sleep(5)  # Check every 5 seconds

    def _check_camera_health(self, cam_id):
        """Check if a camera is healthy based on FFmpeg process state"""
        cam_info = next((c for c in self.available if c["id"] == cam_id), None)
        if not cam_info:
            return

        try:
            # Check if camera is active
            if cam_id in self.active:
                worker = self.active[cam_id]
                # Check if FFmpeg process is running
                if worker.stream_process and worker.stream_process.poll() is None:
                    self.camera_health[cam_id] = {
                        "status": "connected",
                        "last_checked": datetime.now(),
                        "connected": True,
                        "error_message": None
                    }
                else:
                    self.camera_health[cam_id] = {
                        "status": "disconnected",
                        "last_checked": datetime.now(),
                        "connected": False,
                        "error_message": "FFmpeg process not running"
                    }
            else:
                # Camera not active, check if it can be opened (basic availability)
                test_cap = cv2.VideoCapture(cam_info["source"])
                if test_cap.isOpened():
                    ret, _ = test_cap.read()
                    if ret:
                        self.camera_health[cam_id] = {
                            "status": "available",
                            "last_checked": datetime.now(),
                            "connected": False,
                            "error_message": None
                        }
                    else:
                        self.camera_health[cam_id] = {
                            "status": "error",
                            "last_checked": datetime.now(),
                            "connected": False,
                            "error_message": "Cannot read from camera"
                        }
                    test_cap.release()
                else:
                    self.camera_health[cam_id] = {
                        "status": "unavailable",
                        "last_checked": datetime.now(),
                        "connected": False,
                        "error_message": "Camera not accessible"
                    }
        except Exception as e:
            self.camera_health[cam_id] = {
                "status": "error",
                "last_checked": datetime.now(),
                "connected": False,
                "error_message": str(e)
            }

    def list_cameras(self):
        """Return all available cameras with health status"""
        cameras_with_health = []
        for cam in self.available:
            health = self.camera_health.get(cam["id"], {
                "status": "unknown",
                "last_checked": datetime.now(),
                "connected": False,
                "error_message": None
            })
            cam_with_health = cam.copy()
            cam_with_health.update({
                "health": health["status"],
                "connected": health["connected"],
                "active": cam["id"] in self.active,
                "last_checked": health["last_checked"].isoformat(),
                "error_message": health["error_message"]
            })
            cameras_with_health.append(cam_with_health)
        return cameras_with_health

    def start_camera(self, cam_id):
        if cam_id in self.active:
            return self.active[cam_id]

        cam_info = next((c for c in self.available if c["id"] == cam_id), None)
        if not cam_info:
            return None

        worker = FFmpegWorker(cam_id, cam_info["source"], cam_info)
        self.active[cam_id] = worker
        return worker

    def get_camera(self, cam_id):
        return self.active.get(cam_id)

    def reconnect_camera(self, cam_id):
        """Attempt to reconnect a disconnected camera"""
        if cam_id not in self.active:
            # Camera not active, just start it
            return self.start_camera(cam_id)

        # Camera is active, try to restart it
        worker = self.active[cam_id]
        worker.stop()
        del self.active[cam_id]

        # Wait a moment for cleanup
        time.sleep(1)

        # Try to restart
        new_worker = self.start_camera(cam_id)
        return new_worker


camera_manager = CameraManager()
camera_manager.discover()

# ==========================
# Replay Manager
# ==========================
class ReplayManager:
    def __init__(self):
        self.active_replays = {}

    def start_replay(self, filename):
        # Stop any existing replay for this filename
        if filename in self.active_replays:
            self.active_replays[filename].stop()
            del self.active_replays[filename]

        # Only allow plain filenames (no directory components)
        if not filename or os.path.isabs(filename) or os.path.basename(filename) != filename:
            return None

        # Allowlist safe recording filename characters only.
        # This blocks path metacharacters while preserving normal file names.
        if not re.fullmatch(r"[A-Za-z0-9._-]+", filename):
            return None

        safe_filename = os.path.basename(filename)
        base_dir = os.path.realpath(RECORD_DIR)

        # Explicit allowlist: only files currently present in RECORD_DIR are valid.
        valid_filenames = {
            entry for entry in os.listdir(base_dir)
            if os.path.isfile(os.path.join(base_dir, entry))
        }
        if safe_filename not in valid_filenames:
            return None

        filepath = os.path.realpath(os.path.join(base_dir, safe_filename))

        # Ensure the resolved path remains inside RECORD_DIR
        if os.path.commonpath([base_dir, filepath]) != base_dir:
            return None

        if not os.path.exists(filepath):
            return None

        replay_worker = ReplayWorker(safe_filename, filepath)
        self.active_replays[safe_filename] = replay_worker
        return replay_worker

    def get_replay(self, filename):
        return self.active_replays.get(filename)

    def stop_replay(self, filename):
        if filename in self.active_replays:
            self.active_replays[filename].stop()
            del self.active_replays[filename]

    def list_active_replays(self):
        return list(self.active_replays.keys())


class ReplayWorker:
    def __init__(self, filename, filepath):
        self.filename = filename
        self.filepath = filepath
        self.running = True
        self.cap = None
        self.frame = None
        self.lock = threading.Lock()

        # Initialize video capture
        self._init_capture()
        print(f"ReplayWorker started for {filename}")

    def _init_capture(self):
        """Initialize video capture for the replay file"""
        try:
            self.cap = cv2.VideoCapture(self.filepath)
            if not self.cap.isOpened():
                raise Exception(f"Failed to open video file {self.filepath}")

            self.fps = self.cap.get(cv2.CAP_PROP_FPS) or 30.0
            print(f"Replay initialized: {self.filepath} @ {self.fps}fps")
        except Exception as e:
            print(f"Error initializing replay {self.filename}: {str(e)}")
            self.running = False

    def get_frame(self):
        with self.lock:
            return self.frame.copy() if hasattr(self, 'frame') and self.frame is not None else None

    def read_next_frame(self):
        """Read the next frame from the video file"""
        if not self.running or not self.cap:
            return False

        ret, frame = self.cap.read()
        if ret:
            # Add replay overlay
            cv2.putText(frame, "REPLAY", (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 0, 0), 2)
            cv2.putText(frame, f"File: {self.filename}", (10, frame.shape[0] - 10), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)

            with self.lock:
                self.frame = frame
            return True
        else:
            # End of video
            self.running = False
            return False

    def stop(self):
        print(f"Replay {self.filename}: Stopping replay")
        self.running = False
        if self.cap:
            self.cap.release()


replay_manager = ReplayManager()

# ==========================
# Streaming
# ==========================


# ==========================
# FastAPI APIs
# ==========================
@app.get('/cameras')
async def list_cameras():
    """
    Returns all available cameras (USB + IP)
    """
    return JSONResponse(content=camera_manager.list_cameras())


@app.post('/select/{cam_id}')
async def select_camera(cam_id: str):
    """
    Activates a camera
    """
    cam = camera_manager.start_camera(cam_id)
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not found")
    return JSONResponse(content={"status": "camera started", "camera": cam_id, "ready": cam.ready})


@app.get('/camera_status/{cam_id}')
async def camera_status(cam_id: str):
    """
    Returns the current status of a camera
    """
    cam = camera_manager.get_camera(cam_id)
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not active")
    return JSONResponse(content={
        "camera": cam_id,
        "ready": cam.ready,
        "fps": cam.fps,
        "width": cam.width,
        "height": cam.height,
        "uptime": cam.uptime(),
        "error_message": cam.error_message
    })


@app.get('/live_url/{cam_id}')
async def live_url(cam_id: str):
    """
    Returns the RTSP URL for live stream
    """
    cam = camera_manager.get_camera(cam_id)
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not active")
    rtsp_url = f"rtsp://0.0.0.0:8554/live_{cam_id}"
    return JSONResponse(content={"rtsp_url": rtsp_url})


@app.get('/rtsp_url/{cam_id}')
async def rtsp_url(cam_id: str):
    """
    Returns the RTSP URL for live stream (UI endpoint)
    """
    return await live_url(cam_id)


@app.get('/replay_url/{filename}')
async def replay_url(filename: str):
    """
    Returns the RTSP URL for replay stream
    """
    rtsp_url = f"rtsp://0.0.0.0:8554/replay_{filename}"
    return JSONResponse(content={"rtsp_url": rtsp_url})


@app.get('/replay_rtsp_url/{filename}')
async def replay_rtsp_url(filename: str):
    """
    Returns the RTSP URL for replay stream (UI endpoint)
    """
    return await replay_url(filename)


@app.get('/health/{cam_id}')
async def health(cam_id: str):
    """
    Returns the health status of a camera
    """
    health_info = camera_manager.camera_health.get(cam_id)
    if not health_info:
        raise HTTPException(status_code=404, detail="Camera not found")
    return JSONResponse(content=health_info)





@app.post('/start_record/{cam_id}')
async def start_record(cam_id: str):
    cam = camera_manager.get_camera(cam_id)
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not active")
    filename = cam.start_recording()
    return JSONResponse(content={"status": "recording started", "filename": filename})

@app.post('/stop_record/{cam_id}')
async def stop_record(cam_id: str):
    cam = camera_manager.get_camera(cam_id)
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not active")
    cam.stop_recording()
    return JSONResponse(content={"status": "recording stopped"})

@app.post('/stop_stream/{cam_id}')
async def stop_stream(cam_id: str):
    cam = camera_manager.get_camera(cam_id)
    if cam:
        cam.stop()
        del camera_manager.active[cam_id]
    return JSONResponse(content={"status": "stream stopped"})

@app.post('/reconnect/{cam_id}')
async def reconnect_camera(cam_id: str):
    """
    Attempts to reconnect a disconnected camera
    """
    try:
        cam = camera_manager.reconnect_camera(cam_id)
        if cam:
            return JSONResponse(content={"status": "camera reconnected", "camera": cam_id})
        else:
            raise HTTPException(status_code=500, detail="Failed to reconnect camera")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reconnection failed: {str(e)}")

@app.get('/recordings')
async def list_recordings():
    files = []
    for filename in os.listdir(RECORD_DIR):
        filepath = os.path.join(RECORD_DIR, filename)
        if os.path.isfile(filepath):
            mtime = os.path.getmtime(filepath)
            files.append({
                "filename": filename,
                "modified": datetime.fromtimestamp(mtime).isoformat(),
                "size": os.path.getsize(filepath)
            })
    # Sort by modification time, latest first
    files.sort(key=lambda x: x["modified"], reverse=True)
    return JSONResponse(content=files)

@app.post('/start_replay/{filename}')
async def start_replay(filename: str):
    """
    Starts a replay session for the given filename
    """
    replay = replay_manager.start_replay(filename)
    if not replay:
        raise HTTPException(status_code=404, detail="File not found")
    return JSONResponse(content={"status": "replay started", "filename": filename})

@app.post('/stop_replay/{filename}')
async def stop_replay(filename: str):
    """
    Stops a replay session for the given filename
    """
    replay_manager.stop_replay(filename)
    return JSONResponse(content={"status": "replay stopped"})

@app.get('/stream/{cam_id}')
async def stream(cam_id: str):
    """
    Streams the live video from the camera
    """
    cam = camera_manager.get_camera(cam_id)
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not active")

    async def video_stream():
        try:
            while cam.running:
                frame = cam.get_frame()
                if frame is None:
                    await asyncio.sleep(0.1)  # Wait for next frame
                    continue

                ret, jpeg = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 75])
                if ret:
                    yield (b'--frame\r\n'
                           b'Content-Type: image/jpeg\r\n\r\n' + jpeg.tobytes() + b'\r\n')
                await asyncio.sleep(max(0.01, 1 / cam.fps))  # Ensure minimum sleep time
        except Exception as e:
            print(f"Stream error for camera {cam_id}: {e}")

    return StreamingResponse(video_stream(), media_type='multipart/x-mixed-replace; boundary=frame')


@app.get('/replay_stream/{filename}')
async def replay_stream(filename: str):
    """
    Streams the replay video
    """
    filepath = os.path.join(RECORD_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")

    async def replay_video_stream():
        try:
            cap = cv2.VideoCapture(filepath)
            if not cap.isOpened():
                return

            fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
            frame_delay = max(0.01, 1 / fps)

            while True:
                ret, frame = cap.read()
                if not ret:
                    break  # End of video

                # Add replay overlay
                cv2.putText(frame, "REPLAY", (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 0, 0), 2)
                cv2.putText(frame, f"File: {filename}", (10, frame.shape[0] - 10), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)

                ret, jpeg = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 75])
                if ret:
                    yield (b'--frame\r\n'
                           b'Content-Type: image/jpeg\r\n\r\n' + jpeg.tobytes() + b'\r\n')
                await asyncio.sleep(frame_delay)

            cap.release()
        except Exception as e:
            print(f"Replay stream error: {e}")

    return StreamingResponse(replay_video_stream(), media_type='multipart/x-mixed-replace; boundary=frame')

@app.get('/active_replays')
async def list_active_replays():
    """
    Returns list of currently active replays
    """
    return JSONResponse(content=replay_manager.list_active_replays())



@app.post('/ptz/{cam_id}/{action}/{direction}')
async def ptz_control(cam_id: str, action: str, direction: str):
    """
    PTZ control endpoint
    action: 'pan', 'tilt', 'zoom'
    direction: 'left', 'right', 'up', 'down', 'in', 'out'
    """
    cam = camera_manager.get_camera(cam_id)
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not active")

    if action == 'pan':
        if direction in ['left', 'right']:
            cam.pan(direction)
        else:
            raise HTTPException(status_code=400, detail="Invalid pan direction")
    elif action == 'tilt':
        if direction in ['up', 'down']:
            cam.tilt(direction)
        else:
            raise HTTPException(status_code=400, detail="Invalid tilt direction")
    elif action == 'zoom':
        if direction in ['in', 'out']:
            cam.zoom(direction)
        else:
            raise HTTPException(status_code=400, detail="Invalid zoom direction")
    else:
        raise HTTPException(status_code=400, detail="Invalid action")

    return JSONResponse(content={"status": f"{action} {direction} command sent"})

@app.post('/ptz/{cam_id}/stop')
async def ptz_stop(cam_id: str):
    """
    Stop PTZ movement
    """
    cam = camera_manager.get_camera(cam_id)
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not active")

    cam.stop_ptz()
    return JSONResponse(content={"status": "PTZ movement stopped"})

# ==========================
# WebRTC Streaming Endpoints
# ==========================
@app.post('/webrtc_offer/{cam_id}')
async def webrtc_offer(cam_id: str, request: Request):
    """
    Handle WebRTC offer and return answer
    """
    try:
        data = await request.json()
        offer_sdp = data.get('sdp')

        if not offer_sdp:
            raise HTTPException(status_code=400, detail="No SDP provided")

        # Create WebRTC streamer for this camera
        if cam_id not in webrtc_streamers:
            webrtc_streamers[cam_id] = WebRTCStreamer(cam_id)

        streamer = webrtc_streamers[cam_id]

        # Create offer (placeholder - would need full implementation)
        offer = await streamer.create_offer()
        if not offer:
            raise HTTPException(status_code=500, detail="Failed to create WebRTC offer")

        return JSONResponse(content=offer)
    except Exception as e:
        print(f"WebRTC offer error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post('/webrtc_answer/{cam_id}')
async def webrtc_answer(cam_id: str, request: Request):
    """
    Handle WebRTC answer from client
    """
    try:
        data = await request.json()
        answer_sdp = data.get('sdp')

        if not answer_sdp:
            raise HTTPException(status_code=400, detail="No SDP provided")

        # Handle answer (placeholder - would need full implementation)
        if cam_id in webrtc_streamers:
            streamer = webrtc_streamers[cam_id]
            await streamer.handle_answer(answer_sdp)
            return JSONResponse(content={"status": "WebRTC connection established"})
        else:
            raise HTTPException(status_code=404, detail="WebRTC streamer not found")
    except Exception as e:
        print(f"WebRTC answer error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post('/webrtc_network_stats/{cam_id}')
async def webrtc_network_stats(cam_id: str, request: Request):
    """
    Receive network statistics from client and adjust bitrate
    """
    try:
        data = await request.json()
        rtt = data.get('rtt', 0)
        packet_loss = data.get('packet_loss', 0)

        # Get WebRTC streamer for this camera
        if cam_id in webrtc_streamers:
            streamer = webrtc_streamers[cam_id]
            streamer.update_network_stats(rtt, packet_loss)
            return JSONResponse(content={"status": "Network stats updated"})
        else:
            raise HTTPException(status_code=404, detail="WebRTC streamer not found")
    except Exception as e:
        print(f"Network stats update error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/')
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ==========================
# Bootstrap
# ==========================
if __name__ == '__main__':
    config = load_config()
    host = config.get('server', 'host', fallback='0.0.0.0')
    port = config.getint('server', 'port', fallback=5000)
    uvicorn.run(app, host=host, port=port)
