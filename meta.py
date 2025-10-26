# File: new_s.py (Merged Instagram, Facebook, and Threads Uploader)
import os
import time
import logging
import requests
import dropbox
from telegram import Bot
from datetime import datetime
from pytz import timezone
import random
import re

class UnifiedSocialMediaUploader:
    DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token"
    INSTAGRAM_API_BASE = "https://graph.facebook.com/v18.0"
    THREADS_API_BASE = "https://graph.threads.net/v1.0"
    INSTAGRAM_REEL_STATUS_RETRIES = 10
    INSTAGRAM_REEL_STATUS_WAIT_TIME = 15

    def __init__(self):
        self.script_name = "new_s.py"
        self.ist = timezone('Asia/Kolkata')
        self.account_key = "eclipsed_by_you"

        # Logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger()

        # Instagram/Facebook secrets from GitHub environment
        self.meta_token = os.getenv("META_TOKEN")
        self.ig_id = os.getenv("IG_ID")
        self.fb_page_id = os.getenv("FB_PAGE_ID")
        
        # Threads secrets
        self.threads_user_id = os.getenv("THREADS_USER_ID")
        self.threads_access_token = os.getenv("THREADS_ACCESS_TOKEN")
        
        # Telegram configuration
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

        # Dropbox configuration
        self.dropbox_key = os.getenv("DROPBOX_APP_KEY")
        self.dropbox_secret = os.getenv("DROPBOX_APP_SECRET")
        self.dropbox_refresh = os.getenv("DROPBOX_REFRESH_TOKEN")
        self.dropbox_folder = "/ink-wisps"

        if self.telegram_token:
            self.telegram_bot = Bot(token=self.telegram_token)
        else:
            self.telegram_bot = None

        self.start_time = time.time()
        self.session = requests.Session()
        self.log_buffer = []  # Buffer for log messages

    def send_message(self, msg, level=logging.INFO, immediate=False):
        prefix = f"[{self.script_name}]\n"
        full_msg = prefix + msg
        self.log_buffer.append(full_msg)
        try:
            if immediate and self.telegram_bot and self.telegram_chat_id:
                self.telegram_bot.send_message(chat_id=self.telegram_chat_id, text=full_msg)
            # Also log the message to console with the specified level
            if level == logging.ERROR:
                self.logger.error(full_msg)
            else:
                self.logger.info(full_msg)
        except Exception as e:
            self.logger.error(f"Telegram send error for message '{full_msg}': {e}")

    def send_log_summary(self):
        """Send buffered log messages as summary to Telegram."""
        if self.telegram_bot and self.telegram_chat_id and self.log_buffer:
            summary = '\n'.join(self.log_buffer)
            max_len = 4000
            for i in range(0, len(summary), max_len):
                try:
                    self.telegram_bot.send_message(chat_id=self.telegram_chat_id, text=summary[i:i+max_len])
                except Exception as e:
                    self.logger.error(f"Telegram send error: {e}")
        self.log_buffer = []

    def log_console_only(self, msg, level=logging.INFO):
        """Log message to console only, not to Telegram."""
        prefix = f"[{self.script_name}]\n"
        full_msg = prefix + msg
        if level == logging.ERROR:
            self.logger.error(full_msg)
        else:
            self.logger.info(full_msg)

    def refresh_dropbox_token(self):
        self.logger.info("Refreshing Dropbox token...")
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.dropbox_refresh,
            "client_id": self.dropbox_key,
            "client_secret": self.dropbox_secret,
        }
        r = self.session.post(self.DROPBOX_TOKEN_URL, data=data)
        if r.status_code == 200:
            new_token = r.json().get("access_token")
            self.logger.info("Dropbox token refreshed.")
            return new_token
        else:
            self.send_message("❌ Dropbox refresh failed: " + r.text, level=logging.ERROR, immediate=True)
            raise Exception("Dropbox refresh failed.")

    def list_dropbox_files(self, dbx):
        try:
            files = dbx.files_list_folder(self.dropbox_folder).entries
            valid_exts = ('.mp4', '.mov', '.jpg', '.jpeg', '.png')
            return [f for f in files if f.name.lower().endswith(valid_exts)]
        except Exception as e:
            self.send_message(f"❌ Dropbox folder read failed: {e}", level=logging.ERROR, immediate=True)
            return []

    def build_caption_from_filename(self, file):
        """Use filename as caption."""
        base_name = os.path.splitext(file.name)[0]
        base_name = base_name.replace('_', ' ')
        return base_name

    def get_page_access_token(self):
        """Fetch Facebook Page Access Token."""
        try:
            self.log_console_only("🔐 Fetching Page Access Token from Meta API...", level=logging.INFO)
            url = f"https://graph.facebook.com/v18.0/me/accounts"
            params = {"access_token": self.meta_token}
            
            self.log_console_only(f"📡 API URL: {url}", level=logging.INFO)
            
            start_time = time.time()
            res = self.session.get(url, params=params)
            request_time = time.time() - start_time
            
            self.log_console_only(f"⏱️ Page token request completed in {request_time:.2f} seconds", level=logging.INFO)
            self.log_console_only(f"📊 Response status: {res.status_code}", level=logging.INFO)

            if res.status_code != 200:
                self.send_message(f"❌ Failed to fetch Page token: {res.text}", level=logging.ERROR, immediate=True)
                return None

            pages = res.json().get("data", [])
            self.log_console_only(f"🔍 Found {len(pages)} pages in user account", level=logging.INFO)
            
            # Find the target page
            for page in pages:
                page_id = page.get("id", "Unknown")
                page_name = page.get("name", "Unknown")
                page_access_token = page.get("access_token", "Not available")
                
                if page_id == self.fb_page_id:
                    self.log_console_only(f"✅ MATCH FOUND! Target page: {page_name}", level=logging.INFO)
                    
                    if page_access_token and page_access_token != "Not available":
                        self.send_message(f"✅ Page Access Token fetched for: {page_name} (ID: {self.fb_page_id})", immediate=True)
                        self.log_console_only(f"🔐 Using page access token: {page_access_token[:20]}...", level=logging.INFO)
                        return page_access_token
                    else:
                        self.send_message(f"❌ No access token found for page: {page_name}", level=logging.ERROR, immediate=True)
                        return None

            self.send_message(f"⚠️ Page ID {self.fb_page_id} not found in user's account list.", level=logging.ERROR, immediate=True)
            return None
        except Exception as e:
            self.send_message(f"❌ Exception during Page token fetch: {e}", level=logging.ERROR, immediate=True)
            return None

    def get_dropbox_video_metadata(self, dbx, file):
        """Get width, height, duration from Dropbox file metadata."""
        from dropbox.files import VideoMetadata, PhotoMetadata
        metadata = dbx.files_get_metadata(file.path_lower, include_media_info=True)
        if hasattr(metadata, 'media_info') and metadata.media_info:
            info = metadata.media_info.get_metadata()
            width = None
            height = None
            if getattr(info, 'dimensions', None) is not None:
                width = info.dimensions.width
                height = info.dimensions.height
            if isinstance(info, VideoMetadata):
                duration = info.duration / 1000.0  # ms to seconds
            else:
                duration = None
            return width, height, duration
        return None, None, None

    def check_facebook_reel_requirements(self, width, height, duration):
        """
        Check if video meets Facebook Reel requirements (min/max).
        Returns True if meets requirements, False otherwise.
        """
        if width is None or height is None or duration is None:
            return False
        
        aspect_ratio = width / height
        
        # Facebook Reel requirements
        min_height = 960
        min_width = 540
        max_height = 1920
        max_width = 1080
        max_duration = 90
        min_duration = 3
        
        # Check portrait orientation (height > width) and aspect ratio
        is_portrait = height > width
        is_valid_ratio = abs(aspect_ratio - 0.5625) < 0.01  # 9:16
        
        meets_minimum_size = height >= min_height and width >= min_width
        meets_maximum_size = height <= max_height and width <= max_width
        meets_duration = min_duration <= duration <= max_duration
        
        result = is_portrait and is_valid_ratio and meets_minimum_size and meets_maximum_size and meets_duration
        
        self.log_console_only(
            f"📐 Facebook Reel Check:\n"
            f"   Size: {width}x{height} (portrait: {is_portrait})\n"
            f"   Min Size: {min_width}x{min_height} ✓ {meets_minimum_size}\n"
            f"   Max Size: {max_width}x{max_height} ✓ {meets_maximum_size}\n"
            f"   Aspect Ratio: {aspect_ratio:.4f} (valid: {is_valid_ratio})\n"
            f"   Duration: {duration}s (min: {min_duration}s, max: {max_duration}s, valid: {meets_duration})\n"
            f"   Meets all requirements: {result}",
            level=logging.INFO
        )
        
        return result

    def post_to_instagram(self, dbx, file, caption, page_token):
        """Post to Instagram using the provided page token."""
        name = file.name
        ext = name.lower()
        media_type = "REELS" if ext.endswith((".mp4", ".mov")) else "IMAGE"

        self.send_message(f"🚀 Starting Instagram upload: {name}", level=logging.INFO, immediate=True)
        
        temp_link = dbx.files_get_temporary_link(file.path_lower).link
        file_size = f"{file.size / 1024 / 1024:.2f}MB"
        total_files = len(self.list_dropbox_files(dbx))

        self.log_console_only(f"📸 Instagram: {media_type} | Size: {file_size} | Remaining: {total_files}")

        upload_url = f"{self.INSTAGRAM_API_BASE}/{self.ig_id}/media"
        data = {
            "access_token": page_token,
            "caption": caption
        }

        if media_type == "REELS":
            data.update({"media_type": "REELS", "video_url": temp_link, "share_to_feed": "true"})
        else:
            data["image_url"] = temp_link

        self.log_console_only("🔄 Creating Instagram media container...", level=logging.INFO)
        start_time = time.time()
        res = self.session.post(upload_url, data=data)
        request_time = time.time() - start_time
        
        self.log_console_only(f"⏱️ API request completed in {request_time:.2f} seconds", level=logging.INFO)
        self.log_console_only(f"📊 Response status: {res.status_code}", level=logging.INFO)
        
        if res.status_code != 200:
            err = res.json().get("error", {}).get("message", "Unknown")
            self.send_message(f"❌ Instagram upload failed: {err}", level=logging.ERROR, immediate=True)
            return False

        creation_id = res.json().get("id")
        if not creation_id:
            self.send_message(f"❌ No media ID returned for: {name}", level=logging.ERROR, immediate=True)
            return False

        self.log_console_only(f"✅ Media creation successful! Creation ID: {creation_id}", level=logging.INFO)

        # For REELS, poll status
        if media_type == "REELS":
            self.log_console_only("⏳ Processing video for Instagram...", level=logging.INFO)
            processing_start = time.time()
            for attempt in range(self.INSTAGRAM_REEL_STATUS_RETRIES):
                self.log_console_only(f"🔄 Processing attempt {attempt + 1}/{self.INSTAGRAM_REEL_STATUS_RETRIES}", level=logging.INFO)
                
                status_response = self.session.get(
                    f"{self.INSTAGRAM_API_BASE}/{creation_id}?fields=status_code&access_token={page_token}"
                )
                
                if status_response.status_code != 200:
                    self.send_message(f"❌ Status check failed: {status_response.status_code}", level=logging.ERROR, immediate=True)
                    return False
                
                status = status_response.json()
                current_status = status.get("status_code", "UNKNOWN")
                
                self.log_console_only(f"📊 Current status: {current_status}", level=logging.INFO)
                
                if current_status == "FINISHED":
                    processing_time = time.time() - processing_start
                    self.log_console_only(f"✅ Video processing completed in {processing_time:.2f} seconds!", level=logging.INFO)
                    self.log_console_only("⏳ Waiting 15 seconds before publishing...", level=logging.INFO)
                    time.sleep(15)
                    break
                elif current_status == "ERROR":
                    self.send_message(f"❌ Instagram processing failed: {name}", level=logging.ERROR, immediate=True)
                    return False
                
                self.log_console_only(f"⏳ Waiting {self.INSTAGRAM_REEL_STATUS_WAIT_TIME} seconds...", level=logging.INFO)
                time.sleep(self.INSTAGRAM_REEL_STATUS_WAIT_TIME)

        # Publish to Instagram
        self.log_console_only("📤 Publishing to Instagram...", level=logging.INFO)
        publish_url = f"{self.INSTAGRAM_API_BASE}/{self.ig_id}/media_publish"
        publish_data = {"creation_id": creation_id, "access_token": page_token}
        
        publish_start = time.time()
        pub = self.session.post(publish_url, data=publish_data)
        publish_time = time.time() - publish_start
        
        self.log_console_only(f"⏱️ Publish completed in {publish_time:.2f} seconds", level=logging.INFO)
        self.log_console_only(f"📊 Publish status: {pub.status_code}", level=logging.INFO)
        
        if pub.status_code == 200:
            response_data = pub.json()
            instagram_id = response_data.get("id", "Unknown")
            
            if not instagram_id:
                self.send_message("⚠️ Instagram publish succeeded but no media ID returned", level=logging.WARNING, immediate=True)
                return False
            else:
                self.send_message(f"✅ Instagram published!\n📸 Media ID: {instagram_id}\n📦 Remaining: {total_files - 1}", immediate=True)
                # Verify the post is live
                self.verify_instagram_post_by_media_id(instagram_id, page_token)
                return True
        else:
            error_msg = pub.json().get("error", {}).get("message", "Unknown error")
            self.send_message(f"❌ Instagram publish failed: {error_msg}", level=logging.ERROR, immediate=True)
            return False

    def post_to_facebook_page(self, dbx, file, caption, page_token):
        """Post to Facebook with conditional Reel/Video logic based on orientation requirements."""
        media_url = dbx.files_get_temporary_link(file.path_lower).link
        
        if not self.fb_page_id:
            self.send_message("⚠️ Facebook Page ID not configured, skipping Facebook post", level=logging.WARNING, immediate=True)
            return False
        
        if not page_token:
            self.log_console_only("🔐 Fetching fresh Facebook Page Access Token...", level=logging.INFO)
            page_token = self.get_page_access_token()
            if not page_token:
                self.send_message("❌ Could not retrieve Facebook Page access token.", level=logging.ERROR, immediate=True)
                return False

        # Check if file is image or video
        image_exts = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp')
        is_image = file.name.lower().endswith(image_exts)
        
        if is_image:
            # Post image as photo
            return self.post_facebook_photo(file, media_url, caption, page_token)
        
        # Get video metadata
        width, height, duration = self.get_dropbox_video_metadata(dbx, file)
        
        # Check if meets Facebook Reel requirements
        as_reel = self.check_facebook_reel_requirements(width, height, duration)
        
        if as_reel:
            return self.post_facebook_reel(file, media_url, caption, page_token)
        else:
            return self.post_facebook_video(file, media_url, caption, page_token)

    def post_facebook_reel(self, file, media_url, caption, page_token):
        """Post video as Facebook Reel."""
        self.log_console_only("📘 Starting Facebook Reel upload...", level=logging.INFO)
        
        # Step 1: Start upload session
        start_url = f"https://graph.facebook.com/v23.0/{self.fb_page_id}/video_reels"
        start_data = {"upload_phase": "start", "access_token": page_token}
        start_res = self.session.post(start_url, data=start_data)
        
        if start_res.status_code != 200:
            self.send_message(f"❌ Failed to start Facebook Reels upload: {start_res.text}", level=logging.ERROR, immediate=True)
            return False
        
        video_id = start_res.json().get("video_id")
        upload_url = start_res.json().get("upload_url")
        
        if not video_id or not upload_url:
            self.send_message(f"❌ No video_id or upload_url returned: {start_res.text}", level=logging.ERROR, immediate=True)
            return False
        
        # Step 2: Upload video using hosted file (Dropbox temp link)
        headers = {
            "Authorization": f"OAuth {page_token}",
            "file_url": media_url
        }
        upload_res = self.session.post(upload_url, headers=headers)
        
        if upload_res.status_code != 200:
            self.send_message(f"❌ Facebook Reels upload failed: {upload_res.text}", level=logging.ERROR, immediate=True)
            return False
        
        # Step 3: Finish and publish
        finish_data = {
            "upload_phase": "finish",
            "access_token": page_token,
            "video_id": video_id,
            "description": caption,
            "video_state": "PUBLISHED",
            "share_to_feed": "true"
        }
        finish_res = self.session.post(start_url, data=finish_data)
        
        if finish_res.status_code == 200:
            response_data = finish_res.json()
            fb_video_id = response_data.get("id", video_id)
            self.send_message(f"✅ Facebook Reel published!\n📘 Video ID: {fb_video_id}", immediate=True)
            self.verify_facebook_post_by_video_id(fb_video_id, page_token)
            return True
        else:
            self.send_message(f"❌ Facebook Reels publish failed: {finish_res.text}", level=logging.ERROR, immediate=True)
            return False

    def post_facebook_video(self, file, media_url, caption, page_token):
        """Post video as regular Facebook video."""
        self.log_console_only("📘 Starting Facebook Page video upload...", level=logging.INFO)
        
        post_url = f"https://graph.facebook.com/{self.fb_page_id}/videos"
        data = {
            "access_token": page_token,
            "file_url": media_url,
            "description": caption
        }
        
        self.log_console_only(f"📄 Page ID: {self.fb_page_id}", level=logging.INFO)
        self.log_console_only(f"📹 Video URL: {media_url[:50]}...", level=logging.INFO)
        
        res = self.session.post(post_url, data=data)
        
        self.log_console_only(f"📊 Response status: {res.status_code}", level=logging.INFO)
        
        if res.status_code == 200:
            response_data = res.json()
            video_id = response_data.get("id", "Unknown")
            self.send_message(f"✅ Facebook video published!\n📘 Video ID: {video_id}", immediate=True)
            self.verify_facebook_post_by_video_id(video_id, page_token)
            return True
        else:
            error_msg = res.json().get("error", {}).get("message", "Unknown error")
            self.send_message(f"❌ Facebook video upload failed: {error_msg}", level=logging.ERROR, immediate=True)
            return False

    def post_facebook_photo(self, file, media_url, caption, page_token):
        """Post image as Facebook photo."""
        self.log_console_only("🖼️ Starting Facebook photo upload...", level=logging.INFO)
        
        post_url = f"https://graph.facebook.com/{self.fb_page_id}/photos"
        data = {
            "access_token": page_token,
            "url": media_url,
            "caption": caption
        }
        
        res = self.session.post(post_url, data=data)
        
        if res.status_code == 200:
            photo_id = res.json().get("id", "Unknown")
            self.send_message(f"✅ Facebook photo published!\n🖼️ Photo ID: {photo_id}", immediate=True)
            return True
        else:
            error_msg = res.json().get("error", {}).get("message", "Unknown error")
            self.send_message(f"❌ Facebook photo upload failed: {error_msg}", level=logging.ERROR, immediate=True)
            return False

    def post_to_threads(self, dbx, file, caption):
        """Post to Threads using the threads access token."""
        name = file.name.lower()
        media_type = "VIDEO" if name.endswith((".mp4", ".mov")) else "IMAGE"

        temp_link = dbx.files_get_temporary_link(file.path_lower).link
        total_files = len(self.list_dropbox_files(dbx))

        self.send_message(f"🚀 Uploading to Threads: {file.name}\n📐 Type: {media_type}\n📦 Remaining: {total_files}", immediate=True)

        # Extract topic tag from caption
        topic_tag = self.extract_first_hashtag(caption)

        post_url = f"{self.THREADS_API_BASE}/{self.threads_user_id}/threads"
        data = {
            "access_token": self.threads_access_token,
            "text": caption,
            "topic_tag": topic_tag if topic_tag else None,
        }

        # Remove None keys
        data = {k: v for k, v in data.items() if v is not None}

        if temp_link:
            if media_type == "VIDEO":
                data["video_url"] = temp_link
                data["media_type"] = "VIDEO"
            else:
                data["image_url"] = temp_link
                data["media_type"] = "IMAGE"
            
            # Step 1: Create media container
            res = self.session.post(post_url, data=data)
            if res.status_code != 200:
                self.send_message(f"❌ Threads media container creation failed: {res.text}", level=logging.ERROR, immediate=True)
                return False
            
            creation_id = res.json().get("id")
            if not creation_id:
                self.send_message(f"❌ No creation_id returned", level=logging.ERROR, immediate=True)
                return False
            
            # Step 2: Poll status until fully processed
            max_retries = 60
            for attempt in range(max_retries):
                poll_res = self.session.get(
                    f"{self.THREADS_API_BASE}/{creation_id}",
                    params={"access_token": self.threads_access_token}
                )
                if poll_res.status_code != 200:
                    self.send_message(f"❌ Polling failed: {poll_res.text}", level=logging.ERROR, immediate=True)
                    return False
                
                status = poll_res.json().get("status")
                if status == "FINISHED":
                    self.send_message("✅ Threads video processing FINISHED, waiting 3 seconds...", immediate=True)
                    time.sleep(3)
                    break
                elif status == "ERROR":
                    self.send_message(f"❌ Threads transcoding failed: {poll_res.text}", level=logging.ERROR, immediate=True)
                    return False
                
                time.sleep(4)
            
            # Step 3: Publish
            publish_url = f"{self.THREADS_API_BASE}/{self.threads_user_id}/threads_publish"
            publish_data = {
                "access_token": self.threads_access_token,
                "creation_id": creation_id
            }
            
            max_wait = 180
            interval = 5
            start_time = time.time()
            
            while True:
                pub_res = self.session.post(publish_url, data=publish_data)
                if pub_res.status_code == 200:
                    thread_id = pub_res.json().get('id', 'Unknown')
                    self.send_message(f"✅ Threads post published! ID: {thread_id}", immediate=True)
                    return thread_id  # Return ID for verification
                else:
                    error_msg = pub_res.json().get("error", {}).get("message", "Unknown error")
                    self.send_message(f"❌ Threads publish failed: {error_msg}. Retrying...", level=logging.ERROR, immediate=True)
                    
                    if time.time() - start_time > max_wait:
                        self.send_message(f"❌ Threads publish failed after {max_wait} seconds", level=logging.ERROR, immediate=True)
                        return False
                    
                    time.sleep(interval)
            
            return False
        else:
            # Text-only post
            data["media_type"] = "TEXT_POST"
            res = self.session.post(post_url, data=data)
            if res.status_code == 200:
                thread_id = res.json().get('id', 'Unknown')
                self.send_message(f"✅ Threads text post published! ID: {thread_id}", immediate=True)
                return thread_id  # Return ID for verification
            else:
                self.send_message(f"❌ Threads post failed: {res.text}", level=logging.ERROR, immediate=True)
                return False

    def extract_first_hashtag(self, text):
        """Extract the first hashtag (without #) from text for use as topic tag."""
        match = re.search(r"#(\w+)", text)
        return match.group(1) if match else None

    def verify_instagram_post_by_media_id(self, media_id, page_token):
        """Verify Instagram post is live by polling the published media_id."""
        try:
            self.log_console_only("🔍 Verifying Instagram post is live...", level=logging.INFO)
            
            url = f"{self.INSTAGRAM_API_BASE}/{media_id}"
            params = {
                "fields": "id,permalink_url,media_type,media_url,thumbnail_url,created_time",
                "access_token": page_token
            }
            
            for attempt in range(10):
                self.log_console_only(f"🔄 Verification attempt {attempt + 1}/10", level=logging.INFO)
                
                res = self.session.get(url, params=params)
                if res.status_code == 200:
                    post_data = res.json()
                    permalink = post_data.get("permalink_url", "Not available")
                    self.log_console_only(f"✅ Instagram post verified as live!\n🔗 {permalink}", level=logging.INFO)
                    return True
                elif res.status_code == 400:
                    self.log_console_only(f"❌ Unrecoverable error: {res.status_code}", level=logging.INFO)
                    break
                else:
                    self.log_console_only(f"❌ Verification failed: {res.status_code}", level=logging.INFO)
                    if attempt < 9:
                        time.sleep(5)
            
            self.send_message("⚠️ Could not verify Instagram post is live after 10 attempts", level=logging.WARNING)
            return False
            
        except Exception as e:
            self.send_message(f"❌ Exception verifying Instagram post: {e}", level=logging.ERROR)
            return False

    def verify_facebook_post_by_video_id(self, video_id, page_token):
        """Verify Facebook video post is live by polling the video_id."""
        try:
            self.log_console_only("🔍 Verifying Facebook post is live...", level=logging.INFO)
            
            url = f"https://graph.facebook.com/{video_id}"
            params = {
                "fields": "id,permalink_url,created_time,length,title,description",
                "access_token": page_token
            }
            
            for attempt in range(10):
                self.log_console_only(f"🔄 Verification attempt {attempt + 1}/10", level=logging.INFO)
                
                res = self.session.get(url, params=params)
                if res.status_code == 200:
                    post_data = res.json()
                    permalink = post_data.get("permalink_url", "Not available")
                    self.log_console_only(f"✅ Facebook post verified as live!\n🔗 {permalink}", level=logging.INFO)
                    return True
                elif res.status_code == 400:
                    self.log_console_only(f"❌ Unrecoverable error: {res.status_code}", level=logging.INFO)
                    break
                else:
                    self.log_console_only(f"❌ Verification failed: {res.status_code}", level=logging.INFO)
                    if attempt < 9:
                        time.sleep(5)
            
            self.send_message("⚠️ Could not verify Facebook post is live after 10 attempts", level=logging.WARNING)
            return False
            
        except Exception as e:
            self.send_message(f"❌ Exception verifying Facebook post: {e}", level=logging.ERROR)
            return False

    def verify_threads_post(self, thread_id):
        """Verify Threads post is live by polling the thread_id."""
        try:
            self.log_console_only("🔍 Verifying Threads post is live...", level=logging.INFO)
            
            url = f"{self.THREADS_API_BASE}/{thread_id}"
            params = {
                "access_token": self.threads_access_token
            }
            
            for attempt in range(10):
                self.log_console_only(f"🔄 Verification attempt {attempt + 1}/10", level=logging.INFO)
                
                res = self.session.get(url, params=params)
                if res.status_code == 200:
                    post_data = res.json()
                    # Threads API returns post data if successful
                    self.log_console_only(f"✅ Threads post verified as live!\n📄 Thread ID: {thread_id}", level=logging.INFO)
                    return True
                elif res.status_code == 400:
                    self.log_console_only(f"❌ Unrecoverable error: {res.status_code}", level=logging.INFO)
                    break
                else:
                    self.log_console_only(f"❌ Verification failed: {res.status_code}", level=logging.INFO)
                    if attempt < 9:
                        time.sleep(5)
            
            self.send_message("⚠️ Could not verify Threads post is live after 10 attempts", level=logging.WARNING)
            return False
            
        except Exception as e:
            self.send_message(f"❌ Exception verifying Threads post: {e}", level=logging.ERROR)
            return False

    def check_token_expiry(self):
        """Check Meta token expiry and validate before starting."""
        try:
            self.log_console_only("🔍 Checking token expiry...", level=logging.INFO)
            url = "https://graph.facebook.com/debug_token"
            params = {
                "input_token": self.meta_token,
                "access_token": self.meta_token
            }
            
            res = self.session.get(url, params=params)
            if res.status_code != 200:
                self.send_message(f"❌ Failed to check token: {res.text}", level=logging.ERROR, immediate=True)
                return False
                
            data = res.json().get("data", {})
            is_valid = data.get("is_valid", False)
            expires_at = data.get("expires_at")
            
            if not is_valid:
                self.send_message("❌ Token is invalid or expired!", level=logging.ERROR, immediate=True)
                return False
            
            if expires_at:
                expiry_dt = datetime.utcfromtimestamp(expires_at)
                delta = expiry_dt - datetime.utcnow()
                self.log_console_only(f"✅ Token valid - Expires: {expiry_dt.strftime('%Y-%m-%d')} ({delta.days} days left)", level=logging.INFO)
            else:
                self.log_console_only("✅ Token valid - Does not expire", level=logging.INFO)
            
            return True
            
        except Exception as e:
            self.send_message(f"❌ Token check failed: {e}", level=logging.ERROR, immediate=True)
            return False

    def check_instagram_page_connection(self, page_token):
        """Check if Instagram account is properly connected to the Facebook page."""
        try:
            self.log_console_only("🔍 Checking Instagram-Facebook connection...", level=logging.INFO)
            
            url = f"https://graph.facebook.com/v18.0/{self.fb_page_id}"
            params = {
                "fields": "instagram_business_account,connected_instagram_account",
                "access_token": page_token
            }
            
            res = self.session.get(url, params=params)
            if res.status_code != 200:
                self.send_message(f"❌ Failed to check Instagram connection: {res.text}", level=logging.ERROR, immediate=True)
                return False
            
            data = res.json()
            instagram_business_account = data.get("instagram_business_account", {})
            connected_instagram = data.get("connected_instagram_account", {})
            
            if instagram_business_account:
                instagram_id = instagram_business_account.get("id", "Unknown")
                self.log_console_only(f"✅ Instagram Business Account connected: {instagram_id}", level=logging.INFO)
                
                if instagram_id == self.ig_id:
                    self.log_console_only("✅ Instagram ID matches", level=logging.INFO)
                    return True
                else:
                    self.send_message(f"⚠️ Instagram ID mismatch! Connected: {instagram_id}, Expected: {self.ig_id}", level=logging.WARNING, immediate=True)
                    return False
            elif connected_instagram:
                self.log_console_only("✅ Instagram Account connected", level=logging.INFO)
                return True
            else:
                self.send_message("❌ No Instagram account connected!", level=logging.ERROR, immediate=True)
                return False
                
        except Exception as e:
            self.send_message(f"❌ Exception checking connection: {e}", level=logging.ERROR, immediate=True)
            return False

    def authenticate_dropbox(self):
        """Authenticate with Dropbox and return the client."""
        try:
            access_token = self.refresh_dropbox_token()
            return dropbox.Dropbox(oauth2_access_token=access_token)
        except Exception as e:
            self.send_message(f"❌ Dropbox authentication failed: {e}", level=logging.ERROR, immediate=True)
            raise

    def process_file(self, dbx):
        """Process a single file and post to all platforms."""
        files = self.list_dropbox_files(dbx)
        if not files:
            self.log_console_only("📭 No files found in Dropbox folder.", level=logging.INFO)
            return False

        # Pick random file
        file = random.choice(files)
        self.log_console_only(f"🎯 Processing: {file.name}", level=logging.INFO)
        
        # Build caption from filename for all platforms
        caption = self.build_caption_from_filename(file)
        
        results = {
            'instagram': False,
            'facebook': False,
            'threads': False
        }
        
        try:
            # Get page token for Instagram/Facebook (both use same Meta token)
            page_token = self.get_page_access_token()
            if not page_token:
                self.send_message("❌ Could not retrieve Page access token. Aborting Instagram/Facebook.", level=logging.ERROR, immediate=True)
                results['instagram'] = False
                results['facebook'] = False
            else:
                # Check Instagram connection before posting
                if not self.check_instagram_page_connection(page_token):
                    self.send_message("❌ Instagram not properly connected. Aborting Instagram.", level=logging.ERROR, immediate=True)
                    results['instagram'] = False
                else:
                    # Post to Instagram
                    results['instagram'] = self.post_to_instagram(dbx, file, caption, page_token)
                
                # Post to Facebook
                results['facebook'] = self.post_to_facebook_page(dbx, file, caption, page_token)
            
            # Post to Threads
            thread_id = self.post_to_threads(dbx, file, caption)
            if thread_id and thread_id != 'Unknown':
                results['threads'] = True
                # Verify Threads post is live
                self.verify_threads_post(thread_id)
            else:
                results['threads'] = False
            
        except Exception as e:
            self.send_message(f"❌ Exception during post: {e}", level=logging.ERROR, immediate=True)
        
        # Delete file after posting
        try:
            dbx.files_delete_v2(file.path_lower)
            self.log_console_only(f"🗑️ Deleted: {file.name}")
        except Exception as e:
            self.log_console_only(f"⚠️ Failed to delete: {e}", level=logging.WARNING)
        
        # Report results
        status_report = []
        if results['instagram']:
            status_report.append("Instagram ✅")
        else:
            status_report.append("Instagram ❌")
            
        if results['facebook']:
            status_report.append("Facebook ✅")
        else:
            status_report.append("Facebook ❌")
            
        if results['threads']:
            status_report.append("Threads ✅")
        else:
            status_report.append("Threads ❌")
        
        self.send_message(f"📊 Final Status:\n" + " | ".join(status_report), immediate=True)
        
        return any(results.values())  # Return True if any platform succeeded

    def run(self):
        """Main execution method."""
        self.log_console_only(f"📡 Run started: {datetime.now(self.ist).strftime('%Y-%m-%d %H:%M:%S')}", level=logging.INFO)
        
        try:
            # Validate token before starting
            token_valid = self.check_token_expiry()
            if not token_valid:
                self.send_message("❌ Token validation failed. Stopping execution.", level=logging.ERROR, immediate=True)
                return
            
            # Authenticate with Dropbox
            dbx = self.authenticate_dropbox()
            
            # Process one file (caption comes from filename)
            success = self.process_file(dbx)
            
            if success:
                self.send_message("🎉 Social media posting completed!", level=logging.INFO, immediate=True)
            else:
                self.send_message("❌ All platforms failed.", level=logging.ERROR, immediate=True)
            
        except Exception as e:
            self.send_message(f"❌ Script crashed: {e}", level=logging.ERROR, immediate=True)
            raise
        finally:
            # Send summary
            self.send_log_summary()
            duration = time.time() - self.start_time
            self.log_console_only(f"🏁 Run complete in {duration:.1f} seconds", level=logging.INFO)

if __name__ == "__main__":
    UnifiedSocialMediaUploader().run()
