#!/usr/bin/env python3
"""
ig_bridge.py — InstaReach v3 Enhanced
Comprehensive error handling, detection avoidance, and safety features
"""
import sys, json, os, base64, tempfile, time, random


def load_client(username, session_file, password=None):
    """Load Instagram client with session management and error handling"""
    from instagrapi import Client
    cl = Client()
    # Human-like delays (2-5 seconds between API calls)
    cl.delay_range = [2, 5]
    
    # Load existing session if available
    if session_file and os.path.exists(session_file):
        try:
            cl.load_settings(session_file)
            cl.account_info()
            cl.dump_settings(session_file)
            return cl
        except Exception:
            # Session expired or invalid, fall through to login
            pass
    
    # Fresh login with password
    if password:
        try:
            cl.login(username, password)
            if session_file:
                os.makedirs(os.path.dirname(session_file), exist_ok=True)
                cl.dump_settings(session_file)
            return cl
        except Exception as e:
            err_str = str(e).lower()
            if 'challenge' in err_str:
                raise RuntimeError(f"Challenge required for @{username}. Verify via Instagram app.")
            elif '401' in err_str or 'invalid' in err_str.lower():
                raise RuntimeError(f"Invalid credentials for @{username}")
            else:
                raise RuntimeError(f"Login failed for @{username}: {e}")
    
    raise RuntimeError(f"No valid session and no password for @{username}")


def prepare_image(image_b64, image_ext):
    """
    Decode base64 image, optimize for Instagram, save to tmp file.
    Instagram DM photo specs: Square or landscape (4:5 to 1.91:1 aspect ratio)
    """
    if not image_b64:
        return None
    
    try:
        img_bytes = base64.b64decode(image_b64)
    except Exception as e:
        raise RuntimeError(f"Invalid base64 image: {e}")
    
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(img_bytes))
        
        # Convert to RGB (removes alpha channel if PNG)
        if img.mode in ('RGBA', 'P', 'LA'):
            bg = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            bg.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
            img = bg
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Optimize dimensions for Instagram DM
        w, h = img.size
        
        # Ensure minimum size (320px) and reasonable max (2000px)
        min_size = 320
        max_size = 2000
        
        if w < min_size or h < min_size:
            scale = max(min_size/w, min_size/h)
            w, h = int(w*scale), int(h*scale)
        
        if w > max_size or h > max_size:
            scale = min(max_size/w, max_size/h)
            w, h = int(w*scale), int(h*scale)
        
        img = img.resize((w, h), Image.LANCZOS)
        
        # Save as JPEG with high quality for DM
        buf = io.BytesIO()
        img.save(buf, format='JPEG', quality=92)
        img_bytes = buf.getvalue()
        image_ext = 'jpg'
        
    except ImportError:
        # PIL not available — use raw bytes
        pass
    except Exception as e:
        raise RuntimeError(f"Image processing error: {e}")
    
    tmp = tempfile.NamedTemporaryFile(suffix=f'.{image_ext}', delete=False)
    tmp.write(img_bytes)
    tmp.close()
    return tmp.name


def classify_error(error_str):
    """Classify Instagram errors for appropriate response"""
    err_lower = error_str.lower()
    
    # Rate limiting
    if '429' in error_str or 'throttle' in err_lower or 'rate' in err_lower:
        return 'rate_limited'
    
    # Session/auth issues
    if 'challenge' in err_lower or '401' in error_str or 'unauthorized' in err_lower:
        return 'session_expired'
    
    # User not found
    if 'not found' in err_lower or 'user_not_found' in err_lower:
        return 'user_not_found'
    
    # Blocked
    if 'block' in err_lower or 'restricted' in err_lower:
        return 'blocked'
    
    # Spam detection
    if 'spam' in err_lower or 'action_blocked' in err_lower:
        return 'spam_detected'
    
    return 'unknown'

def cmd_login(data):
    """Login to Instagram account"""
    try:
        username = data["username"]
        password = data.get("password", "")
        session_file = data.get("session_file", "")

        cl = load_client(username, session_file, password)

        # Verify successful login
        user_info = cl.account_info()

        user_id = str(getattr(user_info, "pk", ""))

        follower_count = (
            getattr(user_info, "follower_count", None)
            or getattr(user_info, "followers_count", None)
        )

        if follower_count is None:
            try:
                profile = cl.user_info_by_username(username)
                follower_count = (
                    getattr(profile, "follower_count", None)
                    or getattr(profile, "followers_count", None)
                    or 0
                )
            except Exception:
                follower_count = 0

        return {
            "ok": True,
            "username": username,
            "user_id": user_id,
            "follower_count": follower_count,
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "reason": "login_failed"
        }


def cmd_search(data):
    """Search for users by hashtag and direct search"""
    username = data["username"]
    password = data.get("password", "")
    session_file = data.get("session_file", "")
    keyword = data["keyword"]
    
    try:
        cl = load_client(username, session_file, password)
        users = set()
        
        # Search via hashtags (most reliable)
        hashtag = keyword.replace(" ", "").replace("-", "").lower()
        hashtag2 = keyword.replace(" ", "_").lower()
        
        search_strategies = [
            (cl.hashtag_medias_recent, hashtag, 30),
            (cl.hashtag_medias_top, hashtag, 20),
            (cl.hashtag_medias_recent, hashtag2, 20),
        ]
        
        for fn, arg, amt in search_strategies:
            try:
                for m in fn(arg, amount=amt):
                    if hasattr(m, 'user') and m.user:
                        users.add(m.user.username)
                    if len(users) >= 20:
                        break
            except Exception:
                pass
            
            if len(users) >= 20:
                break
            
            # Add human-like delay between searches
            time.sleep(random.uniform(1, 3))
        
        # Fallback: direct user search
        if len(users) < 5:
            try:
                for u in cl.search_users(keyword, count=15):
                    users.add(u.username)
            except Exception:
                pass
        
        return {
            "ok": True,
            "users": list(users),
            "count": len(users),
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "users": [],
            "reason": classify_error(str(e))
        }


def cmd_send_dm(data):
    """Send direct message with optional image"""
    username = data["username"]
    password = data.get("password", "")
    session_file = data.get("session_file", "")
    to_username = data["to_username"]
    message = data["message"]
    image_b64 = data.get("image_b64", "").strip()
    image_ext = data.get("image_ext", "jpg")
    
    tmp_path = None
    image_warning = None
    
    try:
        cl = load_client(username, session_file, password)
        
        # Get recipient user ID
        try:
            user_id = cl.user_id_from_username(to_username)
        except Exception as e:
            if 'not found' in str(e).lower():
                return {
                    "ok": False,
                    "reason": "user_not_found",
                    "error": f"User @{to_username} not found"
                }
            raise e
        
        # Send image if provided
        if image_b64:
            try:
                tmp_path = prepare_image(image_b64, image_ext)
                from pathlib import Path
                cl.direct_send_photo(Path(tmp_path), user_ids=[user_id])
                # Add human-like delay
                time.sleep(random.uniform(1, 2))
            except Exception as ie:
                # Preserve warning but continue with text
                image_warning = f"Image send failed: {ie}"
            finally:
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.unlink(tmp_path)
                    except:
                        pass
                    tmp_path = None
        
        # Send text message
        if message:
            cl.direct_send(message, user_ids=[user_id])
        
        return {
            "ok": True,
            "message_sent": True,
            "to_username": to_username,
            "image_warning": image_warning,
        }
    
    except Exception as e:
        error_reason = classify_error(str(e))
        return {
            "ok": False,
            "reason": error_reason,
            "error": str(e),
        }
    
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except:
                pass


def cmd_inbox(data):
    """Fetch inbox messages from recent conversations"""
    try:
        username = data["username"]
        password = data.get("password", "")
        session_file = data.get("session_file", "")
        
        cl = load_client(username, session_file, password)
        messages = []
        
        # Fetch recent threads
        for thread in cl.direct_threads(amount=20):
            # Get the other user in the thread
            other = thread.users[0] if thread.users else None
            if not other:
                continue
            
            # Extract messages from them
            for msg in thread.messages:
                # Check if message is from the other user and has text
                if str(msg.user_id) == str(other.pk) and msg.text:
                    messages.append({
                        "from_username": other.username,
                        "text": msg.text,
                        "timestamp": str(msg.timestamp),
                    })
            
            # Add human-like delay between thread fetches
            time.sleep(random.uniform(0.5, 1.5))
        
        return {
            "ok": True,
            "messages": messages,
            "message_count": len(messages),
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "messages": [],
            "reason": classify_error(str(e))
        }


def cmd_check_session(data):
    """Verify if session is still valid"""
    try:
        username = data["username"]
        session_file = data.get("session_file", "")
        
        if not session_file or not os.path.exists(session_file):
            return {
                "ok": False,
                "valid": False,
                "reason": "no_session_file"
            }
        
        from instagrapi import Client
        cl = Client()
        cl.load_settings(session_file)
        
        # Quick validation
        cl.account_info()
        
        return {
            "ok": True,
            "valid": True,
            "username": username,
        }
    except Exception as e:
        return {
            "ok": False,
            "valid": False,
            "reason": classify_error(str(e)),
            "error": str(e)
        }


def main():
    try:
        data = json.loads(sys.stdin.read().strip())
    except Exception:
        print(json.dumps({"ok": False, "error": "Invalid JSON"}))
        sys.exit(1)
    
    handlers = {
        "login": cmd_login,
        "search": cmd_search,
        "send_dm": cmd_send_dm,
        "inbox": cmd_inbox,
        "check_session": cmd_check_session,
    }
    
    handler = handlers.get(data.get("cmd", ""))
    if not handler:
        print(json.dumps({
            "ok": False,
            "error": f"Unknown command: {data.get('cmd')}"
        }))
        sys.exit(1)
    
    result = handler(data)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
