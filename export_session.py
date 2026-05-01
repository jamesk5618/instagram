#!/usr/bin/env python3
"""
Instagram Session Exporter for InstaReach v3 (FIXED)
Copy this entire file and save as: export_session.py
Then run: python export_session.py
"""

import json
import os
import sys
from pathlib import Path

def print_header(text):
    """Print formatted header"""
    print("\n" + "="*60)
    print(f"  {text}")
    print("="*60 + "\n")

def print_success(text):
    """Print success message"""
    print(f"✅ {text}")

def print_error(text):
    """Print error message"""
    print(f"❌ {text}")

def print_warning(text):
    """Print warning message"""
    print(f"⚠️  {text}")

def print_info(text):
    """Print info message"""
    print(f"ℹ️  {text}")

def check_instagrapi_installed():
    """Check if instagrapi is installed"""
    try:
        import instagrapi
        return True
    except ImportError:
        return False

def install_instagrapi():
    """Guide user to install instagrapi"""
    print_error("instagrapi is not installed")
    print("\n" + "="*60)
    print("INSTALLATION INSTRUCTIONS:")
    print("="*60)
    print("\n1. Open Command Prompt / Terminal")
    print("2. Run this command:")
    print("\n   pip install instagrapi")
    print("\n3. Wait for installation to complete")
    print("4. Run this script again")
    print("\n" + "="*60)
    sys.exit(1)

def export_session(username, password):
    """
    Export Instagram session using instagrapi
    Returns: path to session file or None if failed
    """
    from instagrapi import Client
    
    print_info(f"Attempting to log in as @{username}...")
    
    try:
        # Create client
        cl = Client()
        
        # Attempt login
        print("🔐 Logging in... (this may take 10-30 seconds)")
        cl.login(username, password)
        
        print_success("Login successful!")
        
        # Get user info (FIXED: handle missing attributes)
        try:
            user_info = cl.account_info()
            
            # Safely get attributes with fallbacks
            full_name = getattr(user_info, 'full_name', 'Unknown')
            username_str = getattr(user_info, 'username', username)
            follower_count = getattr(user_info, 'follower_count', 0)
            following_count = getattr(user_info, 'following_count', 0)
            pk = getattr(user_info, 'pk', 'Unknown')
            
            print_info(f"Account: {full_name} (@{username_str})")
            print_info(f"User ID: {pk}")
            
            if follower_count > 0:
                print_info(f"Followers: {follower_count:,}")
            
            if following_count > 0:
                print_info(f"Following: {following_count:,}")
            
        except Exception as e:
            print_warning(f"Could not fetch full account info: {e}")
            print_info("This is OK - session will still work!")
        
        # Save session
        session_filename = f"session_{username.lower()}.json"
        cl.dump_settings(session_filename)
        
        # Verify file was created
        if not os.path.exists(session_filename):
            print_error(f"Session file was not created!")
            return None
        
        file_size = os.path.getsize(session_filename) / 1024
        print_success(f"Session exported to: {session_filename} ({file_size:.2f} KB)")
        
        return session_filename
        
    except Exception as e:
        error_msg = str(e).lower()
        
        if 'challenge' in error_msg or 'checkpoint' in error_msg:
            print_error("Challenge required - Instagram needs verification")
            print("\n" + "="*60)
            print("WHAT TO DO:")
            print("="*60)
            print("1. Open Instagram app on your phone")
            print("2. Log in and complete any security challenges")
            print("3. Wait 24 hours")
            print("4. Try again")
            print("="*60)
        
        elif 'incorrect' in error_msg or 'invalid' in error_msg:
            print_error(f"Invalid credentials - check your password")
            print("\nTips:")
            print("  • Password is case-sensitive")
            print("  • If 2FA is enabled, use app password instead")
            print("  • Verify you can log in at instagram.com")
        
        elif 'rate' in error_msg or 'throttle' in error_msg:
            print_error("Rate limited - too many login attempts")
            print("\nTips:")
            print("  • Wait 1-2 hours before trying again")
            print("  • Check if account is locked on Instagram.com")
        
        else:
            print_error(f"Login failed: {e}")
        
        return None

def main():
    """Main function"""
    print_header("Instagram Session Exporter for InstaReach v3")
    
    # Check if instagrapi is installed
    if not check_instagrapi_installed():
        install_instagrapi()
    
    # Get credentials from user
    print("Enter your Instagram credentials:")
    print("(Password will NOT be saved - only session is exported)\n")
    
    username = input("📝 Instagram username (no @): ").strip().lower()
    password = input("🔑 Instagram password: ").strip()
    
    # Validate input
    if not username or not password:
        print_error("Username and password are required!")
        sys.exit(1)
    
    if '@' in username:
        username = username.replace('@', '')
        print_warning(f"Removed @ symbol - using: {username}")
    
    # Export session
    session_file = export_session(username, password)
    
    if not session_file:
        print_error("Failed to export session")
        sys.exit(1)
    
    # Show next steps
    print_header("NEXT STEPS")
    print("1. Upload session file to Hostinger:")
    print(f"   File: {session_file}")
    print(f"   Destination: data/sessions/{session_file}")
    print()
    print("2. Steps to upload:")
    print("   • Go to Hostinger hPanel")
    print("   • Click: Hosting → File Manager")
    print("   • Create folder: data/sessions (if doesn't exist)")
    print(f"   • Upload {session_file} into that folder")
    print()
    print("3. Add account to InstaReach:")
    print("   • Go to: SETUP → Accounts → Add Single Account")
    print(f"   • Username: {username}")
    print("   • Password: (leave EMPTY)")
    print("   • Click: + Add")
    print()
    print("4. Test it:")
    print("   • Go to: SETUP → Campaigns")
    print("   • Create a test campaign")
    print("   • Click ▶ Run")
    print("   • Check Engine Logs for: ✓ logged in")
    print()
    print("="*60)
    print_success("All done! Follow the steps above to get started.")
    print("="*60 + "\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Cancelled by user")
        sys.exit(0)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)