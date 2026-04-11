import os, sys, time, json, requests, shutil, subprocess, hashlib, threading, signal
from pathlib import Path
import libtorrent as lt
from tqdm import tqdm
from video_encryptor import encrypt_file
from concurrent.futures import ThreadPoolExecutor, as_completed

# =======================================================================
# 📡 SYSTEM SYNC (Cell-Pasting & Standalone Compatible)
# =======================================================================
# This block intelligently pulls config from Colab globals OR environment
def get_config(name, default=None):
    return globals().get(name) or os.environ.get(name) or default

BACKEND_URL = get_config("BACKEND_URL")
ADMIN_KEY = get_config("ADMIN_KEY")
WORKER_ID = get_config("WORKER_ID") or get_config("WORKER_NAME")
GITHUB_TOKEN = get_config("GITHUB_TOKEN")
GITHUB_USERNAME = get_config("GITHUB_USERNAME")
MASTER_ENCRYPTION_KEY = get_config("MASTER_ENCRYPTION_KEY")
WORKER_MODE = int(get_config("WORKER_MODE", 1)) # 1 = Production, 2 = Speed Testing (Dumb CQ)

# Validation
missing = [k for k, v in {"BACKEND_URL": BACKEND_URL, "ADMIN_KEY": ADMIN_KEY, "WORKER_ID": WORKER_ID, "MASTER_ENCRYPTION_KEY": MASTER_ENCRYPTION_KEY}.items() if not v]
if missing:
    print(f"❌ Titan Error: Missing required variables: {', '.join(missing)}. Please set them in your Credentials cell!")

HEADERS = { "Authorization": f"Bearer {ADMIN_KEY}", "Content-Type": "application/json" }

class TitanEngine:
    def __init__(self):
        self.workspace = Path("/content/titan_v4")
        self.workspace.mkdir(exist_ok=True)
        # Use RAM Disk for all transient I/O to avoid VM disk bottlenecks
        self.output_base = Path("/dev/shm/titan_output") if sys.platform.startswith("linux") else self.workspace / "output"
        self.has_gpu = self._check_gpu()
        self.status = "IDLE"
        self.progress = 0
        self.current_task_id = None
        self._setup_git()

    def _check_gpu(self):
        try:
            subprocess.run(["nvidia-smi"], check=True, capture_output=True)
            return True
        except: return False

    def _setup_git(self):
        self.safe_git_op(["git", "config", "--global", "user.email", "titan@aniclip.site"], cwd=self.workspace)
        self.safe_git_op(["git", "config", "--global", "user.name", "TitanWorker"], cwd=self.workspace)

    def get_audio_map(self, input_file):
        try:
            cmd = ["ffprobe", "-v", "error", "-select_streams", "a", "-show_entries", "stream=index:stream_tags=language", "-of", "json", str(input_file)]
            res = json.loads(subprocess.check_output(cmd))
            streams = res.get('streams', [])
            if not streams: return None
            for i, stream in enumerate(streams):
                if stream.get('tags', {}).get('language', '').lower() in ['jpn', 'jap']: return f"0:a:{i}"
            return "0:a:0"
        except: return None

    def safe_git_op(self, cmd, cwd, retries=3):
        """Standardized Git operator with retries and hard-reset recovery on pull failure"""
        for attempt in range(retries):
            try:
                # Add username/token to remote URLs if they are detected in the command or derived from it
                # Logic for pull: If it fails, we fetch and reset hard
                res = subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True)
                return res
            except subprocess.CalledProcessError as e:
                print(f"⚠️ Git Op Failed (Attempt {attempt+1}/{retries}): {' '.join(cmd)}")
                print(f"   Error: {e.stderr.strip()}")
                
                # Special recovery for 'git pull' failure
                if cmd[1] == 'pull' and attempt < retries - 1:
                    print("   🔄 Attempting Hard Reset recovery...")
                    try:
                        subprocess.run(["git", "fetch", "--all"], cwd=cwd, check=True)
                        subprocess.run(["git", "reset", "--hard", "origin/main"], cwd=cwd, check=True)
                        continue # Retry the pull (or skip if reset was enough)
                    except: pass
                
                if attempt == retries - 1:
                    raise e
                time.sleep(5 * (attempt + 1))

    def generate_clip_id(self, mal_id, ep_num, start_ms, end_ms):
        base = f"{mal_id}-{ep_num}-{start_ms}-{end_ms}"
        hash_8 = hashlib.sha1(base.encode()).hexdigest()[:8]
        return f"{base}-{hash_8}"

    def update_heartbeat(self, msg):
        try:
            requests.post(f"{BACKEND_URL}/worker/heartbeat", json={
                "worker_id": WORKER_ID,
                "task_id": self.current_task_id,
                "status": self.status,
                "status_message": msg,
                "progress": self.progress
            }, headers=HEADERS, timeout=10)
        except Exception as e:
            # Silence heartbeat logs in main thread to avoid tqdm flicker
            pass

class BackgroundHeartbeat(threading.Thread):
    def __init__(self, engine):
        super().__init__()
        self.engine = engine
        self.daemon = True
        self.stop_event = threading.Event()

    def run(self):
        print("💓 Background Heartbeat Active")
        while not self.stop_event.is_set():
            try:
                msg = f"{self.engine.status}: {self.engine.progress}%"
                requests.post(f"{BACKEND_URL}/worker/heartbeat", json={
                    "worker_id": WORKER_ID,
                    "task_id": self.engine.current_task_id,
                    "status": self.engine.status,
                    "status_message": msg,
                    "progress": self.engine.progress
                }, headers=HEADERS, timeout=10)
            except: pass
            time.sleep(30)

    def stop(self):
        self.stop_event.set()

    def handle_exit(self, signum=None, frame=None):
        """Releases the current task back to the queue on exit or crash"""
        if self.current_task_id:
            print(f"\n⚠️ Titan Interrupted. Releasing Task {self.current_task_id}...")
            try:
                # Use /tasks/ not /production/tasks/ if BACKEND_URL already includes /production
                # Or use a fallback logic. Let's try /tasks/ first assuming BACKEND_URL points to /api/production
                res = requests.post(f"{BACKEND_URL}/tasks/{self.current_task_id}/reset", headers=HEADERS, timeout=5)
                if res.status_code == 200:
                    print("✅ Task released successfully.")
                else:
                    # Try fallback if the first one failed (in case of URL mapping differences)
                    requests.post(f"{BACKEND_URL.replace('/production', '')}/production/tasks/{self.current_task_id}/reset", headers=HEADERS, timeout=5)
            except Exception as e:
                print(f"❌ Failed to release task: {e}")
        sys.exit(0)

    def download_torrent_file(self, magnet, target_file_path):
        self.status = "DOWNLOADING"
        self.progress = 0
        
        # Critical Fix: Enable DHT to allow magnet links to resolve
        settings = {'listen_interfaces': '0.0.0.0:6881', 'enable_dht': True}
        ses = lt.session(settings)
        ses.add_dht_router("router.utorrent.com", 6881)
        ses.add_dht_router("router.bittorrent.com", 6881)
        ses.add_dht_router("dht.transmissionbt.com", 6881)
        ses.add_dht_router("router.bitcomet.com", 6881)
        
        params = lt.parse_magnet_uri(magnet)
        params.save_path = str(self.workspace)
        h = ses.add_torrent(params)
        
        print(f"🧲 Metadata acquisition (Searching DHT)...")
        wait_counts = 0
        while not h.status().has_metadata:
            s = h.status()
            print(f"   [DHT] Peers: {s.num_peers} | State: {s.state}", end="\r")
            time.sleep(1)
            wait_counts += 1
            if wait_counts > 120:
                raise Exception("Metadata acquisition timed out. Magnet link has no seeds or DHT is blocked.")
        
        print(f"\n✅ Metadata Acquired! ({wait_counts}s)")
        
        info = h.torrent_file()
        # Fallback just in case the binding version still requires the old method
        if not info:
            info = h.get_torrent_info()
            
        files = info.files()
        target_idx = -1
        
        priorities = [0] * info.num_files()
        for i in range(info.num_files()):
            if target_file_path in files.file_path(i):
                target_idx = i
                priorities[i] = 7
                break
        
        if target_idx == -1:
            raise Exception(f"File '{target_file_path}' not found in torrent.")
            
        h.prioritize_files(priorities)
        
        print(f"⏬ Downloading: {target_file_path}")
        target_size = files.file_size(target_idx)
        
        with tqdm(total=target_size, unit='B', unit_scale=True, desc="Titan Download", unit_divisor=1024) as pbar:
            while not h.status().is_seeding:
                s = h.status()
                
                # Update Dashboard Status
                self.progress = int(s.progress * 100)
                self.update_heartbeat(f"DL: {round(s.download_rate/1024/1024, 2)} MB/s")
                
                # Update Local Progress Bar
                file_progs = h.file_progress()
                downloaded = file_progs[target_idx]
                pbar.n = downloaded
                pbar.set_postfix(speed=f"{round(s.download_rate/1024/1024, 2)} MB/s")
                pbar.refresh()
                
                if target_size > 0 and downloaded >= target_size:
                    print(f"\n   ✅ File downloaded 100%")
                    break
                    
                time.sleep(2)
            
        return self.workspace / target_file_path

    def process_segment(self, i, task, video_path, audio_map, hq_enc, v_enc, hq_q, prev_q, hover_q, hq_pix_fmt, base_norm, hover_norm):
        """Worker function for parallel segment generation and encryption"""
        start_ms = i * 10000
        end_ms = (i + 1) * 10000
        ss_float = start_ms / 1000.0
        dur = 10.0
        cid = self.generate_clip_id(task['mal_id'], task['episode_num'], start_ms, end_ms)
        
        # 1. THE MEGA-COMMAND: 4 Assets, 1 Pass decoding using -map
        # We keep the fast/accurate seek method (-ss before -i) as requested by user.
        # Audio is handled properly through mapping or dropping
        thumb_seek = ss_float + (dur * 0.25)
        
        audio_args = ["-map", audio_map, "-c:a", "aac"] if audio_map else ["-an"]
        
        cmd = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-ss", str(ss_float), "-t", str(dur), "-i", str(video_path),
            "-map_metadata", "-1", "-map_chapters", "-1",
            
            # HQ Output
            *base_norm, "-pix_fmt", hq_pix_fmt, "-map", "v:0", *audio_args, 
            "-c:v", hq_enc, "-preset", "p1", *hq_q, f"{self.output_base}/{cid}_hq.mp4",
            
            # Preview Output
            *base_norm, "-pix_fmt", "yuv420p", "-map", "v:0", *audio_args, 
            "-vf", "scale=-2:520:flags=fast_bilinear", "-c:v", v_enc, "-preset", "p1", *prev_q, f"{self.output_base}/{cid}_prev.mp4",
            
            # Hover Output (5s max)
            *hover_norm, "-map", "v:0", "-t", "5", "-an",
            "-vf", "scale=-2:160:flags=fast_bilinear", "-c:v", v_enc, "-preset", "p1", *hover_q, f"{self.output_base}/{cid}_hover.mp4"
        ]
        
        # Adding WebP thumbnail generation directly to the mega-command causes output buffering issues with image formats
        # We will keep it as a very fast secondary process on the RAM disk
        cmd_thumb = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-ss", str(thumb_seek), "-i", str(video_path), 
            "-vframes", "1", "-vf", "scale=-2:160", "-c:v", "libwebp", "-q:v", "60", f"{self.output_base}/{cid}.webp"
        ]

        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            subprocess.run(cmd_thumb, check=True, capture_output=True, text=True)
            
             # --- 20MB Guard Pass 2: Fallback ---
            if WORKER_MODE == 1:
                fallback_args = {
                    "hq": ["-c:v", hq_enc, "-maxrate", "10M", "-bufsize", "20M"],
                    "prev": ["-c:v", v_enc, "-maxrate", "10M", "-bufsize", "20M"],
                    "hover": ["-c:v", v_enc, "-maxrate", "5M", "-bufsize", "10M"]
                }
                for out_f, pass_args in fallback_args.items():
                    fpath = f"{self.output_base}/{cid}_{out_f}.mp4"
                    if os.path.exists(fpath) and os.path.getsize(fpath) > 20_000_000:
                        tmp_path = f"{self.output_base}/{cid}_{out_f}_tmp.mp4"
                        shutil.move(fpath, tmp_path)
                        fallback_cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", tmp_path, *pass_args, "-c:a", "copy", fpath]
                        try:
                            subprocess.run(fallback_cmd, check=True, capture_output=True, text=True)
                            os.remove(tmp_path)
                        except: pass
            
            
            # 2. ENCRYPTION LAYER (Within Thread to parallelize CPU-heavy crypto)
            episode_token = f"{task['mal_id']}-{task['episode_num']}"
            out_files_map = {}
            for f_key, temp_path in [
                ("hq", f"{self.output_base}/{cid}_hq.mp4"),
                ("preview", f"{self.output_base}/{cid}_prev.mp4"),
                ("hover", f"{self.output_base}/{cid}_hover.mp4"),
                ("thumbnail", f"{self.output_base}/{cid}.webp")
            ]:
                if os.path.exists(temp_path):
                    enc_path = encrypt_file(temp_path, MASTER_ENCRYPTION_KEY, episode_token, cid)
                    os.remove(temp_path)
                    out_files_map[f_key] = f"{task['anime_slug']}/ep_{task['episode_num']}/{os.path.basename(enc_path)}"
            
            return {
                "start_ms": start_ms, "end_ms": end_ms,
                "storage_id": task['storage']['id'],
                "sources": out_files_map
            }
            
        except subprocess.CalledProcessError as e:
            print(f"\n❌ FFmpeg Segment {i} Crash Detail:\n{e.stderr}\n")
            return None
        except Exception as e:
            print(f"Error processing segment {i}: {e}")
            return None

    def run_job(self, task):
        self.current_task_id = task['task_id']
        
        # Start background heartbeat to prevent watchdog timeouts
        hb = BackgroundHeartbeat(self)
        hb.start()
        
        try:
            video_path = self.download_torrent_file(task['source_url'], task['file_path'])
            
            self.status = "EXTRACTING"
            self.progress = 0
            
            probe = subprocess.check_output(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", str(video_path)])
            duration = int(float(probe.decode().strip()))
            
            # Color Depth Detection
            fmt = 'unknown'
            bits = '8'
            try:
                depth_probe = subprocess.check_output([
                    "ffprobe", "-v", "error", "-select_streams", "v:0", 
                    "-show_entries", "stream=pix_fmt,bits_per_raw_sample", 
                    "-of", "json", str(video_path)
                ])
                depth_data = json.loads(depth_probe.decode())['streams'][0]
                fmt = depth_data.get('pix_fmt', 'unknown')
                bits = depth_data.get('bits_per_raw_sample', '8') # Default to 8 if not reported
                print(f"🎬 Video Analysis: {fmt} ({bits}-bit color depth)")
                self.update_heartbeat(f"Analysis: {fmt} {bits}bit")
            except:
                print("🎬 Video Analysis: Color depth detection failed (assuming 8-bit)")
            
            clips_metadata = []
            if self.output_base.exists(): shutil.rmtree(self.output_base)
            self.output_base.mkdir(parents=True)
            
            is_10bit = (bits == '10' or '10le' in fmt)
            
            if WORKER_MODE == 1:
                # --- MODE 1: PRODUCTION (Dynamic Bitrate, 20MB Guards, 10-Bit Preservation) ---
                if is_10bit:
                    hq_enc = "hevc_nvenc" if self.has_gpu else "libx265"
                    hq_pix_fmt = "yuv420p10le"
                    hq_q = ["-rc", "vbr", "-cq", "16", "-b:v", "0"] if self.has_gpu else ["-crf", "17"]
                else:
                    hq_enc = "h264_nvenc" if self.has_gpu else "libx264"
                    hq_pix_fmt = "yuv420p"
                    hq_q = ["-rc", "vbr", "-cq", "16", "-b:v", "0"] if self.has_gpu else ["-crf", "17"]

                v_enc = "h264_nvenc" if self.has_gpu else "libx264"
                
                # Predictive 20MB Guard
                hq_q = hq_q + ["-maxrate", "15M", "-bufsize", "30M"]
                prev_q = ["-rc", "vbr", "-cq", "24", "-b:v", "0", "-maxrate", "8M", "-bufsize", "16M"] if self.has_gpu else ["-crf", "23", "-maxrate", "8M", "-bufsize", "16M"]
                hover_q = ["-rc", "vbr", "-cq", "30", "-b:v", "0", "-maxrate", "4M", "-bufsize", "8M"] if self.has_gpu else ["-crf", "30", "-maxrate", "4M", "-bufsize", "8M"]
            
            else:
                # --- MODE 2: SPEED TESTING (Dumb Constant Quantizer, 8-Bit Forced, No Maxrate) ---
                hq_enc = "h264_nvenc" if self.has_gpu else "libx264"
                v_enc = hq_enc
                hq_pix_fmt = "yuv420p"
                
                hq_q = ["-qp", "16"] if self.has_gpu else ["-crf", "17"]
                prev_q = ["-qp", "24"] if self.has_gpu else ["-crf", "23"]
                hover_q = ["-qp", "30"] if self.has_gpu else ["-crf", "30"]
            
            base_norm = ["-r", "24", "-g", "24", "-keyint_min", "24", "-force_key_frames", "expr:gte(t,n_forced*1)"]
            hover_norm = ["-r", "10", "-g", "10", "-keyint_min", "10", "-force_key_frames", "expr:gte(t,n_forced*1)", "-pix_fmt", "yuv420p"]
            
            audio_map = self.get_audio_map(video_path)
            
            total_segments = duration // 10
            if WORKER_MODE == 3:
                print(f"🚀 FLASH MODE ACTIVE (WORKER_MODE=3). Limiting processing to 2 segments.")
                total_segments = min(total_segments, 2)

            # Smart Concurrency limits to 3 concurrent sessions for NVIDIA T4 limits, otherwise higher for CPU processing
            max_workers = 3 if self.has_gpu else 8
            print(f"🚀 Turbo-Processing {total_segments} segments (Pool Threads: {max_workers})...")
            
            repo_local = self.workspace / "repo"
            completed_segments = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(self.process_segment, i, task, video_path, audio_map, hq_enc, v_enc, hq_q, prev_q, hover_q, hq_pix_fmt, base_norm, hover_norm)
                    for i in range(total_segments)
                ]
                
                for future in tqdm(as_completed(futures), total=total_segments, desc="Titan Tasks"):
                    result = future.result()
                    if result: 
                        clips_metadata.append(result)
                        
                    completed_segments += 1
                    self.progress = int((completed_segments / total_segments) * 100)
                    
                    # --- Multi-Repo Spillover System (Every 10 segments processed) ---
                    if completed_segments % 10 == 0:
                        self.update_heartbeat(f"Processed Segment {completed_segments}/{total_segments}")
                        if repo_local.exists():
                            dest = repo_local / task['anime_slug'] / f"ep_{task['episode_num']}"
                            dest.mkdir(parents=True, exist_ok=True)
                            for f in self.output_base.glob("*"): shutil.copy(f, dest)
                            
                            try:
                                repo_size = sum(f.stat().st_size for f in repo_local.rglob('*') if f.is_file())
                                if repo_size > 1.5 * 1024 * 1024 * 1024: # 1.5 GB
                                    print(f"\n🔄 Repository size ({round(repo_size/1024/1024, 2)} MB) nearing limit. Spilling over...")
                                    self.status = "SYNCING"
                                    self.progress = 0
                                    self.safe_git_op(["git", "add", "."], cwd=repo_local)
                                    self.safe_git_op(["git", "commit", "-m", f"Titan spillover checkpoint"], cwd=repo_local)
                                    self.safe_git_op(["git", "push"], cwd=repo_local)
                                    
                                    shutil.rmtree(repo_local)
                                    for f in self.output_base.glob("*"): os.remove(f)
                                    
                                    res = requests.post(f"{BACKEND_URL}/worker/heartbeat", json={
                                        "worker_id": WORKER_ID, "task_id": self.current_task_id, "status": "WORKING",
                                        "status_message": "Switching Repo Target", "progress": self.progress,
                                        "request_new_storage": True
                                    }, headers=HEADERS).json()
                                    
                                    if "new_storage" in res:
                                        task['storage'] = res['new_storage']
                                        repo_name = f"{task['storage']['name']}".replace(" ", "-")
                                        repo_url = f"https://{GITHUB_TOKEN}@github.com/{GITHUB_USERNAME}/{repo_name}.git"
                                        print(f"📦 Resuming with fresh storage unit: {repo_name}")
                                        self.status = "SYNCING"
                                        self.safe_git_op(["git", "clone", repo_url, str(repo_local)], cwd=self.workspace)
                            except Exception as e:
                                print(f"❌ Storage Spillover Check failed: {e}")
                
            self.status = "SYNCING"
            repo_name = f"{task['storage']['name']}".replace(" ", "-")
            repo_url = f"https://{GITHUB_TOKEN}@github.com/{GITHUB_USERNAME}/{repo_name}.git"
            repo_local = self.workspace / "repo"
            print(f"📦 Synchronizing to remote repository: {GITHUB_USERNAME}/{repo_name}")
            
            if repo_local.exists():
                try:
                    origin_url = subprocess.check_output(["git", "config", "--get", "remote.origin.url"], cwd=repo_local).decode().strip()
                    if repo_name not in origin_url:
                        print(f"🔄 Switching repository target. Cleaning up previous context...")
                        shutil.rmtree(repo_local)
                except Exception:
                    shutil.rmtree(repo_local)

            if not repo_local.exists(): 
                self.status = "SYNCING"
                self.safe_git_op(["git", "clone", repo_url, str(repo_local)], cwd=self.workspace)
            else:
                self.status = "SYNCING"
                self.safe_git_op(["git", "pull"], cwd=repo_local)
            
            dest = repo_local / task['anime_slug'] / f"ep_{task['episode_num']}"
            dest.mkdir(parents=True, exist_ok=True)
            for f in self.output_base.glob("*"): shutil.copy(f, dest)
            
            self.status = "SYNCING"
            self.safe_git_op(["git", "add", "."], cwd=repo_local)
            self.safe_git_op(["git", "commit", "-m", f"Titan ingestion Task: {task['task_id']}"], cwd=repo_local)
            self.safe_git_op(["git", "push"], cwd=repo_local)
            
            resp = requests.post(f"{BACKEND_URL}/worker/ingest", json={
                "task_id": task['task_id'],
                "worker_id": WORKER_ID,
                "anime_id": task['anime_id'],
                "episode_id": task['episode_id'],
                "mal_id": task['mal_id'],
                "episode_num": task['episode_num'],
                "raw_clips": clips_metadata
            }, headers=HEADERS)

            if resp.status_code == 200:
                print(f"✅ Task {task['task_id']} Completed and Ingested.")
            else:
                print(f"❌ Ingest Failed ({resp.status_code}): {resp.text}")
                
        finally:
            self.status = "IDLE"
            self.current_task_id = None
            # Stop heartbeat session
            hb.stop()
            hb.join(timeout=5)
        
    def poll(self):
        print(f"🚀 titan-v4 consumer online. ID: {WORKER_ID} | Mode: {WORKER_MODE}")
        
        # Register signal handlers for graceful exit
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGTERM, self.handle_exit)

        while True:
            try:
                # Hitting /worker/poll. If BACKEND_URL is .../api/production, this becomes .../api/production/worker/poll
                url = f"{BACKEND_URL}/worker/poll?worker_id={WORKER_ID}&worker_name={WORKER_ID}"
                res = requests.get(url, headers=HEADERS, timeout=20)
                
                if res.status_code != 200:
                    print(f"\n❌ Server Error {res.status_code} at {url}")
                    print(f"Response: {res.text[:200]}")
                    time.sleep(15)
                    continue

                try:
                    data = res.json()
                except Exception as e:
                    print(f"\n❌ JSON Decode Error from {url}")
                    print(f"Full Body: {res.text[:500]}")
                    time.sleep(15)
                    continue

                if data.get('task_id'):
                    print(f"⚡ Task Assigned: {data['anime_title']} Ep {data['episode_num']}")
                    try:
                        self.run_job(data)
                    except Exception as e:
                        print(f"\n❌ Job Crash: {e}")
                        self.handle_exit() # Trigger reset on crash
                else:
                    print(f"💤 [{time.strftime('%H:%M:%S')}] Polling... (Idle)", end="\r")
                    time.sleep(15)
            except Exception as e:
                print(f"\nPolling Connection Error: {e}")
                time.sleep(30)

if __name__ == "__main__":
    engine = TitanEngine()
    engine.poll()
